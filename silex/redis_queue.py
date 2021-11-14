#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
    redis_queue
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    基于 Redis 封装的队列

    :author:    lightless <root@lightless.me>
    :homepage:  None
    :license:   GPL-3.0, see LICENSE for more details.
    :copyright: Copyright (c) 2017-2021 lightless. All rights reserved
"""
import platform
import socket
import typing

import redis


class RedisQueue:
    """
    基于 Redis 封装的队列
    该类的每一个实例代表一个 Redis 连接
    """

    def __init__(self, host="localhost", port=6379, password="", db_index=0, queue_name=""):
        super(RedisQueue, self).__init__()

        self.host = host
        self.port = port
        self.password = password
        self.db_index = db_index
        self.queue_name = queue_name

        self._connection: typing.Optional[redis.Redis] = None

    @staticmethod
    def get_connection(host, port, password, db_index, queue_name):
        _queue = RedisQueue(host, port, password, db_index, queue_name)
        return _queue if _queue.connect() else None

    def connect(self):

        # 如果在 120s（TCP_KEEPIDLE）内没有收到任何网络数据
        # 则会每 5s（TCP_KEEPINTVL）发送一个空包
        # 如果连续 3 次（TCP_KEEPCNT）都没有收到 ACK 回复，则认为连接已经失效，抛出异常
        options = {
            socket.TCP_KEEPINTVL: 5,
            socket.TCP_KEEPCNT: 3,
            socket.TCP_KEEPIDLE: 60
        }

        if platform.system() == "Darwin":
            del options[socket.TCP_KEEPINTVL]

        conn = redis.Redis(
            host=self.host, port=self.port, db=self.db_index, password=self.password,
            socket_timeout=15, socket_connect_timeout=15, socket_keepalive=True, socket_keepalive_options=options
        )

        if conn.ping():
            self._connection = conn
            return self
        else:
            return None

    def put_message(self, msg: str):
        return self._connection.lpush(self.queue_name, msg)

    def get_message(self, timeout=1):
        return self._connection.brpop(self.queue_name, timeout)


if __name__ == '__main__':
    queue = RedisQueue("192.168.62.129", 16379, password="123456", db_index=0, queue_name="test_queue")
    if not queue.connect():
        raise RuntimeError("Connect to redis failed.")

    for i in range(10):
        queue.put_message("message: {}".format(i))

    while True:
        msg = queue.get_message()
        if not msg:
            break
        print("receive message: {}".format(msg))
