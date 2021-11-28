#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
    redis_manager
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    $END$

    :author:    lightless <root@lightless.me>
    :homepage:  None
    :license:   GPL-3.0, see LICENSE for more details.
    :copyright: Copyright (c) 2017-2021 lightless. All rights reserved
"""
import platform
import socket
import typing

import redis


class RedisManager:
    def __init__(self, host="localhost", port=6379, password="", db_index=0):
        super(RedisManager, self).__init__()

        self.host = host
        self.port = port
        self.password = password
        self.db_index = db_index

        self._connection: typing.Optional[redis.Redis] = None

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

    def get_redis_connection(self):
        """
        获取原始的 redis 连接实例
        :return:
        """
        return self._connection


class RedisQueue(RedisManager):

    def __init__(self, host="localhost", port=6379, password="", db_index=0, queue_name=""):
        super(RedisQueue, self).__init__(host, port, password, db_index)
        self.queue_name = queue_name

    def put_message(self, msg: str):
        return self._connection.lpush(self.queue_name, msg)

    def get_message(self, timeout=1):
        return self._connection.brpop(self.queue_name, timeout)


if __name__ == '__main__':
    # test redisManager
    r = RedisManager("192.168.62.129", 16379, password="123456", db_index=0)
    if not r.connect():
        raise RuntimeError("connect redis failed.")
    rr = r.get_redis_connection()
    rr.lpush("aa", "1")

    # test redisQueue
    r = RedisQueue("192.168.62.129", 16379, password="123456", db_index=0, queue_name="test_queue")
    if not r.connect():
        raise RuntimeError("connect redis failed.")
    r.put_message("aabb")
    print(f"{r.get_message()=}")
