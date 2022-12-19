from server_config import NetworkConfigs
from queue import Queue

_debug = NetworkConfigs.DEBUG

thread_events = dict()

class CommandQueues:

    def create_queue(self, queue_name):
        if queue_name in self._queue:
            if _debug:
                print('Multi Creation of Command Queue with queue name of ', queue_name)
            return

        self._queue.update({queue_name: Queue()})

    def insert_command(self, queue_name, args):
        if queue_name not in self._queue:
            if _debug:
                print('Command input to undefined Queue')
            return False
        if _debug:
            print("pushing command to queue name : ", queue_name, "args : ", args)
        self._queue[queue_name].put(args)
        thread_events[queue_name].set()
        return True

    def get_task(self, queue_name):
        if queue_name not in self._queue:
            if _debug:
                print('Command input to undefined Queue')
            return None

        if self._queue[queue_name].empty():
            thread_events[queue_name].wait(timeout=10)
            thread_events[queue_name].clear()
            if self._queue[queue_name].empty():
                return None

        cmd = self._queue[queue_name].get()
        if _debug:
            print("get task from", queue_name, "cmd content: ", cmd)
        return cmd

    def is_empty(self, queue_name):
        if queue_name not in self._queue:
            if _debug:
                print('Command input to undefined Queue')
            return False
        return self._queue[queue_name].empty()


    def __init__(self):
        if _debug:
            print("command queue generation")
        self._queue = dict()
        self.create_queue('connection')
        self.create_queue('face_request')
        self.create_queue('user_command')


