import logging
import time

import pika
import dataclasses_serialization as ds

from local_types import *
import json
import unittest

QUEUE = 'books'
HOST ="127.0.0.1"
USER = "books"
PASSWD = "books"


def serialize (book: LBook)-> str:
    from dataclasses_serialization.json import JSONSerializer

    return json.dumps(JSONSerializer.serialize(book))


def deserialize (book_json) -> LBook:
    from dataclasses_serialization.json import JSONSerializer

    return JSONSerializer.deserialize(LBook, book_json)


class TaskWorkerCallBack:
    def process(self, book: LBook):
        pass

class SerializeTest (unittest.TestCase):

    def test1(self):
        b = LBook (filename="test", size=100, doctype="test doctype", tg_id="gggg", dlg_id=10.0, msg_id=100, meta={})
        s = serialize(b)
        print(s)
        b1 = deserialize(json.loads(s))

        print (b1)


class TaskMgr:
    def connect (self):
        pass

    def close (self):
        pass

    def add_task(self, book: LBook):
        pass

    def read_tasks (self, callback: TaskWorkerCallBack):
        pass


class RabbitTaskMgr(TaskMgr):

    def __init__(self, host = HOST, queue = QUEUE, user = USER, passwd = PASSWD):
        self.host = host
        self.queue = queue
        self.user = user
        self.credentials = pika.PlainCredentials(self.user, passwd)
        self.parameters = pika.ConnectionParameters(host=self.host, port='5672', credentials=self.credentials)
        self.connection = None
        self.channel = None

    def __process_message (self, channel, method, props, msg, callback):
        try:
            book = deserialize(json.loads(msg))
            print (book)

            callback(book)
        except BaseException as ex:
            logging.error(f"msg: {msg} {ex}")

    def read_tasks (self, callback):
        cnt = self.channel.queue_declare(queue=self.queue, passive=True).method.message_count
        print(f"messages = {cnt}")

        while True:
            if self.channel is None or not self.channel.is_open:
                self.connect()

            method_frame, header_frame, body = self.channel.basic_get(self.queue)
            if method_frame:
                try:
                    self.__process_message(self.channel, method_frame, header_frame, body, callback),
                    self.channel.basic_ack(method_frame.delivery_tag)
                except BaseException as ex:
                    logging.error(f"read_tasks {ex}")
            else:
                time.sleep(1)



    def read_tasks2 (self, callback):

        if self.channel is None:
            self.connect()

        cnt = self.channel.queue_declare(queue=self.queue, passive=True).method.message_count
        print (f"messages = {cnt}")

        self.channel.basic_consume(queue=self.queue,
                                   on_message_callback=lambda channel, method, props, msg:
                                   self.__process_message(channel,method,props,msg, callback),
                                   auto_ack=False)
        self.channel.start_consuming()

    def connect(self):
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()

    def add_task(self, book: LBook):
        if self.channel is None:
            self.connect()

        book_s = serialize(book)
        print (book_s)
        self.channel.basic_publish(exchange="", routing_key=self.queue, body=book_s)

    def close (self):
        if self.channel:
            self.channel.close()

        if self.connection:
            self.connection.close()


class RabbitTaskMgrTest(unittest.TestCase):

    def setUp(self) -> None:
        self.taskMgr: TaskMgr = RabbitTaskMgr()

    def test1(self) -> None:
        self.taskMgr.connect()
        self.taskMgr.close()

    def test2 (self) -> None:
        b = LBook(filename="test", size=100, doctype="test doctype", tg_id=10, dlg_id=10, msg_id=100)

        self.taskMgr.connect()
        self.taskMgr.add_task(b)
        self.taskMgr.close()

    def test3(self):
        self.taskMgr.connect()
        self.taskMgr.read_tasks(lambda body: print(body))
        self.taskMgr.close()







