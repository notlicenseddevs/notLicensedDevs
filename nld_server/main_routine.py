import sys
import os
a_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(a_path)

from mqtt_client import NldSubscriber, NldPublisher
from mongo_manager import MongoConnection
from server_config import NetworkConfigs
import threading
import atexit

from global_resources import conn_table, cmd_queue
from command_queue import thread_events

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
import base64

from nld_face_recognition.face_req_handler import FaceRequestHandler

import random
import time
_debug = NetworkConfigs.DEBUG


class MqttClientManager:
    # hold mqtt clients for memory management
    _subscriber_list = list()
    _publisher_list = list()

    @classmethod
    def generate_subscriber(cls, c_name=None):
        tmp = NldSubscriber()
        tmp.set_name(c_name)
        cls._subscriber_list.append(tmp)
        return tmp

    @classmethod
    def generate_publisher(cls, c_name=None):
        tmp = NldPublisher()
        tmp.set_name(c_name)
        cls._publisher_list.append(tmp)
        return tmp

    @classmethod
    def client_expiry(cls, c):
        if c in cls._subscriber_list:
            cls._subscriber_list.remove(c)
        elif c in cls._publisher_list:
            cls._publisher_list.remove(c)
        c.expire_connection()
        del c

    @classmethod
    def expire_all(cls):
        for c in cls._publisher_list:
            c.expire_connection()
        for c in cls._subscriber_list:
            c.expire_connection()
        return True

def msg_encrypt(public_key, msg):
    return public_key.encrypt(
        msg,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )

def topic_name_generation(device_id, timestamp, client_pubkey):
    # topic name generation routine
    _topic = str(device_id) + str(timestamp) + str(random.randrange(1000, 9999))
    ##########################

    # practical version #####
    pub_key = serialization.load_pem_public_key(
        client_pubkey,
        backend=default_backend()
    )
    _ret = msg_encrypt(pub_key, bytes(_topic,'ascii'))
    #########################

    return _topic, _ret

def consume_connection_req():
    from mqtt_client import parse_payload

    if _debug:
        print('thread started ',threading.current_thread().getName())
        print("Key file loading")

    with open(a_path+"/keys/private_key.pem", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    def decrypt_to_origin(encrypted):
        return private_key.decrypt(
            encrypted,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    if _debug:
        print("Key file successfully loaded")
    _broadcaster = MqttClientManager.generate_publisher(c_name="Consume Connection Request Broadcaster")
    _replying_topic = "connect/reply"

    def exit_handler():
        if _debug:
            print("exit_handler from",threading.current_thread().getName())

    while True:
        if thread_events['exit'].is_set():
            exit_handler()
            return

        _cmd = cmd_queue.get_task(queue_name="connection")
        if _cmd is None:
            time.sleep(3)
            continue

        try:
            bytes_array = base64.b64decode(_cmd)
            first = bytes_array[:256]
            second = bytes_array[256:512]
            third = bytes_array[512:]
            json_string = decrypt_to_origin(first) + decrypt_to_origin(second) + decrypt_to_origin(third)
            _args = parse_payload(json_string)
        except Exception as e:
            print("Malformed Connection Request.")
            continue

        if _args is None:
            continue

        # remove this instructions later
        if 'MAC' in _args:
            print("Field name changed. Use \"device_id\" instead of \"MAC\"")
            continue

        for con in conn_table:
            if conn_table[con]['device_id'] == _args['device_id']:
                former = conn_table.pop(con)
                if former['user_conn'] is not None:
                    if _debug:
                        print("Duplicated Connection from same device. Expire former connection")
                    MqttClientManager.client_expiry(former['user_conn'])
                break

        client_pubkey = _args['public_key']

        _generated_topic, _encrypted = topic_name_generation(_args['device_id'], _args['timestamp'], bytes(client_pubkey,'ascii'))
        if _debug:
            print('generated topic name : ', _generated_topic)

        user_conn = MqttClientManager.generate_subscriber(str(_args['device_id'])+"User connection")
        tmp = _generated_topic + '/user_command'
        user_conn.subscribe_topic(tmp)

        conn_table[_generated_topic] = \
            {'dev_type': _args['dev_type'], 'user_conn': user_conn, 'device_id' : _args['device_id'],
             'specified_user': False, 'mongo_id':None , 'face_auth': False, 'pin_auth': False, 'app_auth': False}
        if _debug:
            print("Conn Table: ",conn_table)
        _data = {"device_id": _args['device_id'], "topic_name": base64.b64encode(_encrypted).decode('ascii')}

        _broadcaster.publish_data(topic_name=_replying_topic, data=_data)

def consume_user_commands():
    if _debug:
        print('thread started ',threading.current_thread().getName())

    db_conn = MongoConnection()
    data_sender = MqttClientManager.generate_publisher(c_name="Consume User Command Data sender")

    def exit_handler():
        if _debug:
            print("exit_handler from",threading.current_thread().getName())
        db_conn.client.close()

    while True:
        if thread_events['exit'].is_set():
            exit_handler()
            return

        _cmd = cmd_queue.get_task(queue_name="user_command")
        if _cmd is None:
            time.sleep(3)
            continue

        if _debug:
            print('Do task for the command : ', _cmd)

        cmd_type = _cmd['cmd_type']
        if cmd_type == 2:
            # login
            if _debug:
                print(" ******* Login, line_no = 137, from main_routine.py")
            replying_topic = _cmd['base_topic']+'/reply'

            if conn_table[_cmd['base_topic']]['specified_user']:
                continue


            user_doc = db_conn.specify_by_id(_cmd['user_id'])

            if user_doc is None:
                data_sender.publish_data(topic_name=replying_topic, data={'request_type': 1, 'succeed': False})
                continue

            # encrypted login routine needed
            to_kill = None
            for con in conn_table:
                if conn_table[con]['mongo_id'] == user_doc['_id'] and \
                        conn_table[con]['dev_type'] == conn_table[_cmd['base_topic']]['dev_type']:
                    to_kill = con
                    break
            if to_kill is not None:
                print("Duplicated Login from same user")
                MqttClientManager.client_expiry(conn_table[to_kill]['user_conn'])
                data_sender.publish_data(topic_name=to_kill+'/reply', data={'request_type' : 10})
                conn_table.pop(to_kill)


            if user_doc['user_passwd'] != _cmd['passwd']:
                data_sender.publish_data(topic_name=replying_topic, data={'request_type': 1, 'succeed': False})
            else:
                conn_table[_cmd['base_topic']]['mongo_id'] = user_doc['_id']
                conn_table[_cmd['base_topic']]['app_auth'] = True
                conn_table[_cmd['base_topic']]['specified_user'] = True
                data_sender.publish_data(topic_name=replying_topic, data={'request_type': 1, 'succeed': True})


        elif cmd_type == 3:
            # pin auth from center fascia
            if _debug:
                print("********* Pin_auth, line_no = 146, from main_routine.py")
            replying_topic = _cmd['base_topic']+'/reply'
            if not conn_table[_cmd['base_topic']]['specified_user']:
                continue
            mongo_oid = conn_table[_cmd['base_topic']]['mongo_id']
            user_doc = db_conn.find_by_object_id(mongo_oid)

            #encrypted auth routine needed
            #test version
            if user_doc['pin_number'] != _cmd['pin_number']:
                data_sender.publish_data(topic_name=replying_topic, data={'request_type': 2, 'succeed': False})
            else:
                conn_table[_cmd['base_topic']]['pin_auth'] = True
                data_sender.publish_data(topic_name=replying_topic, data={'request_type': 2, 'succeed': True})

        elif cmd_type == 4:
            # refresh from user app
            if _debug:
                print("******** Refresh, line_no = 164, from main_routine.py")
            if not conn_table[_cmd['base_topic']]['specified_user']:
                continue

            user_oid = conn_table[_cmd['base_topic']]['mongo_id']

            target = _cmd['refresh_target']
            gps_items = "-1"
            sw_items = "-1"

            if target not in [0, 1, 2]:
                print('target must be one of [0, 1, 2]')
                continue

            if target <= 1:
                sw_items = dict()
                sw_items['channels'] = db_conn.return_sw_info(user_oid, 0)
                sw_items['playlist'] = db_conn.return_sw_info(user_oid, 1)
            if target != 1:
                gps_items = db_conn.return_sw_info(user_oid, 2)

            _conn_info = conn_table[_cmd['base_topic']]

            if _conn_info['app_auth']:
                if target !=1:
                    data_sender.publish_data(topic_name=_cmd['base_topic']+'/gps_configs', data=gps_items)
                if target <= 1:
                    data_sender.publish_data(topic_name=_cmd['base_topic']+'/sw_configs', data=sw_items)
            else:
                if _conn_info['pin_auth']:
                    data_sender.publish_data(topic_name=_cmd['base_topic']+'/gps_configs', data=gps_items)
                data_sender.publish_data(topic_name=_cmd['base_topic'] + '/sw_configs', data=sw_items)

        elif cmd_type == 5:
            # insert
            if _debug:
                print("******* Insert Object, line_no = 214, from main_routine.py")
            if not conn_table[_cmd['base_topic']]['specified_user']:
                continue
            user_oid = conn_table[_cmd['base_topic']]['mongo_id']
            item_to_push = _cmd['item']
            target_list = _cmd['target_list']
            if target_list not in [0, 1, 2]:
                print('target_list must be one of [0, 1, 2]')
                continue
            after_insertion = db_conn.insert_document(user_oid, target_list, item_to_push)

            to_refresh = list()
            for t in conn_table:
                if conn_table[t]['mongo_id'] == user_oid:
                    to_refresh.append(conn_table[t])

            for target in to_refresh:
                # Note that insertion of hw configuration cannot be happened in this routine
                if target_list == 2:
                    if target['pin_auth'] or target['dev_type'] == 1:
                        data_sender.publish_data(topic_name=_cmd['base_topic']+'/gps_configs', data=after_insertion)
                elif target_list in [0, 1]:
                    dat = {'channels' : None, 'playlist': None}
                    if target_list == 0:
                        dat['channels'] = after_insertion
                    else:
                        dat['playlist'] = after_insertion
                    data_sender.publish_data(topic_name=_cmd['base_topic']+'/sw_configs', data=dat)


        elif cmd_type == 6:
            # delete
            if _debug:
                print("******* Delete object, line_no = 233, from main_routine.py")
            if not conn_table[_cmd['base_topic']]['specified_user']:
                continue
            user_oid = conn_table[_cmd['base_topic']]['mongo_id']
            target_list = _cmd['target_list']
            target_id = _cmd['item_oid']
            if target_list not in [0, 1, 2]:
                print('target_list must be one of [0, 1, 2]')
                continue
            after_deletion = db_conn.delete_document(user_oid, target_list, target_id)

            to_refresh = list()
            for t in conn_table:
                if conn_table[t]['mongo_id'] == user_oid:
                    to_refresh.append(conn_table[t])
            for target in to_refresh:
                # Note that deletion of hw configuration cannot be happened in this routine
                if target_list == 2:
                    if target['pin_auth'] or target['dev_type'] == 1:
                        data_sender.publish_data(topic_name=_cmd['base_topic']+'/gps_configs', data=after_deletion)
                elif target_list in [0, 1]:
                    dat = {'channels': None, 'playlist': None}
                    if target_list == 0:
                        dat['channels'] = after_deletion
                    else:
                        dat['playlist'] = after_deletion
                    data_sender.publish_data(topic_name=_cmd['base_topic'] + '/sw_configs', data=dat)

        elif cmd_type == 7:
            # revise
            if _debug:
                print("******* Revise Object, line_no = 264, from main_routine.py")
            if not conn_table[_cmd['base_topic']]['specified_user']:
                continue
            user_oid = conn_table[_cmd['base_topic']]['mongo_id']
            target_list = _cmd['target_list']
            target_id = _cmd['item_oid']
            revision = _cmd['revision']
            if target_list not in [0, 1, 2, 3]:
                print('target_list must be one of [0, 1, 2]')
                continue
            after_revision = db_conn.modify_document(user_oid, target_list, target_id, revision)
            to_refresh = list()
            for t in conn_table:
                if conn_table[t]['mongo_id'] == user_oid:
                    to_refresh.append(conn_table[t])
            for target in to_refresh:
                # Note that deletion of hw configuration cannot be happened in this routine
                if target_list == 2:
                    if target['pin_auth'] or target['dev_type'] == 1:
                        data_sender.publish_data(topic_name=_cmd['base_topic']+'/gps_configs', data=after_revision)
                elif target_list in [0, 1]:
                    dat = {'channels': None, 'playlist': None}
                    if target_list == 0:
                        dat['channels'] = after_revision
                    else:
                        dat['playlist'] = after_revision
                    data_sender.publish_data(topic_name=_cmd['base_topic'] + '/sw_configs', data=dat)


        elif cmd_type == 8:
            # ready signal
            if _debug:
                print("********* Ready Signal Received, line_no = 296, from main_routine.py")

            if not conn_table[_cmd['base_topic']]['specified_user']:
                continue
            _conn_info = conn_table[_cmd['base_topic']]
            user_oid = _conn_info['mongo_id']

            if _cmd['waiting_for'] == 0:
                # Center Fascia
                hw_items = db_conn.return_hw_info(user_oid)
                sw_items = dict()
                sw_items['channels'] = db_conn.return_sw_info(user_oid, 0)
                sw_items['playlist'] = db_conn.return_sw_info(user_oid, 1)
                data_sender.publish_data(topic_name=_cmd['base_topic']+'/hw_configs', data=hw_items)
                data_sender.publish_data(topic_name=_cmd['base_topic']+'/sw_configs', data=sw_items)
            elif _cmd['waiting_for'] == 1:
                # User App
                gps_items = db_conn.return_sw_info(user_oid, 2)
                sw_items = dict()
                sw_items['channels'] = db_conn.return_sw_info(user_oid, 0)
                sw_items['playlist'] = db_conn.return_sw_info(user_oid, 1)
                data_sender.publish_data(topic_name=_cmd['base_topic']+'/gps_configs', data=gps_items)
                data_sender.publish_data(topic_name=_cmd['base_topic']+'/sw_configs', data=sw_items)

            elif _cmd['waiting_for'] == 2:
                gps_items = db_conn.return_sw_info(user_oid, 2)
                data_sender.publish_data(topic_name=_cmd['base_topic']+'/gps_configs', data=gps_items)

        elif cmd_type == 9:
            # user registration
            if _debug:
                print("********* User Registration, Line_no = 327 from main_routine.py")
            if conn_table[_cmd['base_topic']]['specified_user']:
                continue
            succeed = db_conn.user_registration(_cmd)
            data_sender.publish_data(topic_name=_cmd['base_topic']+'/reply', data={'request_type': 9, 'succeed': succeed})

        elif cmd_type == 10:
            # user exit
            if _debug:
                print("******** User Logout, Line_no = 336 from main_routine.py")
            if not conn_table[_cmd['base_topic']]['specified_user']:
                continue

            # revise received data
            if conn_table[_cmd['base_topic']]['dev_type'] == 0:
                db_conn.update_hw_configs(conn_table[_cmd['base_topic']]['mongo_id'], _cmd['hw_configs'])


            # set all to default
            conn_table[_cmd['base_topic']].update(
                {'specified_user': False, 'mongo_id': None,
                 'face_auth': False, 'pin_auth': False, 'app_auth': False}
            )

        elif cmd_type == 11:
            # expire connection with device
            # expire subscriber
            if _debug:
                print("******** Connection Expiry, Line_no =355 from main_routine.py")
            MqttClientManager.client_expiry(conn_table[_cmd['base_topic']]['user_conn'])
            conn_table.pop(_cmd['base_topic'])
            pass

def consume_face_req():
    if _debug:
        print('thread started ',threading.current_thread().getName())

    db_conn = MongoConnection()
    data_sender = MqttClientManager.generate_publisher(c_name="Consume Face Request Data Sender")
    FaceRequestHandler.handler_init()

    def exit_handler():
        if _debug:
            print("exit_handler from",threading.current_thread().getName())

    while True:
        if thread_events['exit'].is_set():
            exit_handler()
            return

        _cmd = cmd_queue.get_task(queue_name="face_request")
        if _cmd is None:
            time.sleep(3)
            continue
        cmd_type = _cmd['cmd_type']

        if cmd_type == 0:

            # face recognition routnine
            if conn_table[_cmd['base_topic']]['specified_user']:
                continue

            tic = time.time()
            similar_df = FaceRequestHandler.specify_user(_cmd['face_img'], try_best=True)

            print("total = ", time.time()-tic)
            succeed = similar_df is not None
            if similar_df is None or similar_df.empty:
                data_sender.publish_data(topic_name=_cmd['base_topic'] + '/reply',
                                         data={'request_type': 0,
                                               'succeed': False
                                               })
                continue


            conn_table[_cmd['base_topic']]['specified_user'] = succeed
            conn_table[_cmd['base_topic']]['face_auth'] = succeed
            uoid = similar_df.loc[0, 'mongo_id']
            score = similar_df.loc[0, 'VGG-Face_cosine']
            print(score)
            conn_table[_cmd['base_topic']]['mongo_id'] = uoid

            data_sender.publish_data(topic_name=_cmd['base_topic'] + '/reply',
                                     data={'request_type': 0,
                                           'succeed': True,
                                           'user_id': db_conn.find_by_object_id(uoid)['user_id']
                                           })


        if cmd_type == 1:
            if not conn_table[_cmd['base_topic']]['specified_user']:
                continue

            user_oid = conn_table[_cmd['base_topic']]['mongo_id']

            if _cmd['face_img'] is None:
                representation = None
            else:
                tic = time.time()
                representation = FaceRequestHandler.return_representaion(_cmd['face_img'], try_best=True)
                print("total = ", time.time()-tic)
                if representation is None:
                    print("Representation is None")
                    data_sender.publish_data(topic_name=_cmd['base_topic'] + '/reply',
                                             data={'request_type': 3,
                                                   'succeed': False
                                                   })
                    continue
                FaceRequestHandler.register_user_representation(user_oid, representation)
            db_conn.upload_face_image(user_oid, representation)
            data_sender.publish_data(topic_name=_cmd['base_topic'] + '/reply',
                                     data={'request_type': 3,
                                           'succeed': True
                                           })

        if not cmd_queue.is_empty(queue_name="face_request"):
            thread_events['face_request'].set()

def exit_handler():
    MqttClientManager.expire_all()
    thread_events['exit'].set()


def run():

    connection_subscriber = MqttClientManager.generate_subscriber(c_name="Connection Request Listener")
    connection_subscriber.subscribe_topic('connect/request')

    thread_events['exit'] = threading.Event()
    thread_events['face_request'] = threading.Event()
    thread_events['connection'] = threading.Event()
    thread_events['user_command'] = threading.Event()

    atexit.register(exit_handler)


    connection_req_consumer = threading.Thread(
        target=consume_connection_req,
        daemon=True,
        name='connection_req'
    )
    user_command_consumer = threading.Thread(
        target=consume_user_commands,
        daemon=True,
        name='user_command'
    )

    face_req_consumer = threading.Thread(
        target=consume_face_req,
        daemon=True,
        name='face_req'
    )


    connection_req_consumer.start()

    user_command_consumer.start()
    face_req_consumer.start()

    while True:
        time.sleep(3)

    connection_req_consumer.join()
    user_command_consumer.join()
    face_req_consumer.join()

if __name__ == '__main__':
    run()
