import paho.mqtt.client as mqtt
from server_config import NetworkConfigs
import json


from global_resources import conn_table, cmd_queue

_debug = NetworkConfigs.DEBUG


def on_connect(client, userdata, flags, rc):
    if _debug:
        if rc == 0:
            print("Successfully connected")
        else:
            print("Bad connection Returned code=", rc)


def on_disconnect(client, userdata, flags, rc=0):
    if _debug:
        print("Successfully disconnected")


def on_subscribe(client, userdata, mid, granted_qos):
    if _debug:
        print("subscribed: " + str(mid) + " " + str(granted_qos))


def parse_payload(msg_bytes):
    if _debug:
        print("message received ", msg_bytes)

    try:
        tmp = msg_bytes.decode('ascii')
    except UnicodeDecodeError:
        print("Error decoding the message. Check whether the message written in ascii")
        return None


    try:
        data = json.loads(tmp)
    except json.decoder.JSONDecodeError:
        print("Error converting the message to json object. Check whether the format of message written correctly")
        return None
    return data


def user_on_message(client, userdata, msg):

    topic_name = msg.topic
    payload = parse_payload(msg.payload)
    if payload is None :
        return

    topic_args = topic_name.split('/')

    if topic_args[0] not in conn_table:
        return

    arg = {'base_topic': topic_args[0]}
    arg.update(payload)
    if arg['cmd_type'] in [0,1]:
        if _debug:
            print("Command Added to Queue Face_Request")
        cmd_queue.insert_command(queue_name="face_request", args=arg)
    else:
        if _debug:
            print("Command Added to Queue User_Command")
        cmd_queue.insert_command(queue_name="user_command", args=arg)


def conn_on_message(client, userdata, msg):

    topic_name = msg.topic

    if _debug:
        print("Received message from topic name : ", msg.topic)

    if topic_name == 'connect/request':
        # produce task to "consume_connection_req" routine

        #####  test version ######

        cmd_queue.insert_command(queue_name="connection", args=msg.payload)
    else:
        #print(topic_name.split('/')[1])
        pass


def on_publish(client, userdata, mid):
    if _debug:
        print("In on_pub callback mid= ", mid)


class NldSubscriber:
    def __init__(self):
        self._topic = None

        self._client = mqtt.Client()
        if self._client is None:
            raise Exception("mqtt client generation error")

        self.c_name = None
        self._client.on_connect = on_connect
        self._client.on_subscribe = on_subscribe
        self._client.on_message = None
        self._client.on_disconnect = on_disconnect

        self._client.connect(NetworkConfigs.HOST, NetworkConfigs.PORT, keepalive=65535)

        self._client.loop_start()

    def subscribe_topic(self, topic_name):
        # subscribe only one topic at a time
        if self._topic is not None:
            self._client.unsubscribe(topic=self._topic)
        self._topic = topic_name
        if self._topic == 'connect/request':
            self._client.on_message = conn_on_message
        else:
            self._client.on_message = user_on_message
        self._client.subscribe(topic=self._topic, qos=NetworkConfigs.QoS)

        return True

    def now_subscribing(self):
        return self._topic

    def is_alive(self):
        return self._client.is_connected()

    def expire_connection(self):
        if _debug:
            print('expiring connection of ', self.get_name())
        self._client.disconnect()

    def set_name(self, c_name):
        self.c_name = c_name

    def get_name(self):
        if self.c_name is None:
            return "Unnamed Mqtt Subscriber Client"
        else:
            return self.c_name

class NldPublisher:
    def __init__(self):
        self._client = mqtt.Client()
        if self._client is None:
            raise Exception("mqtt client generation error")

        self.c_name = None
        self._client.on_connect = on_connect
        self._client.on_publish = on_publish
        self._client.on_disconnect = on_disconnect
        self._client.connect(NetworkConfigs.HOST, NetworkConfigs.PORT, keepalive=65535)

    def publish_data(self, topic_name, data):
        print("#########PUBLISH DATA", topic_name, data)
        _data = json.dumps(data)
        self._client.publish(topic=topic_name, payload=_data, qos=NetworkConfigs.QoS)
        return True

    def expire_connection(self):
        if _debug:
            print('expiring connection of ', self.get_name())
        self._client.disconnect()

    def set_name(self, c_name):
        self.c_name = c_name

    def get_name(self):
        if self.c_name is None:
            return "Unnamed Mqtt Publisher Client"
        else:
            return self.c_name
