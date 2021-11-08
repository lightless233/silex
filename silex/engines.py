#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
    silex.engines
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    一些常用的多线程封装

    :author:    lightless <root@lightless.me>
    :homepage:  None
    :license:   GPL-3.0, see LICENSE for more details.
    :copyright: Copyright (c) 2017-2021 lightless. All rights reserved
"""
import abc
import multiprocessing
import threading
import typing


class CommonBaseEngine(metaclass=abc.ABC):
    class EngineStatus:
        READY = 0x00
        RUNNING = 0x01
        STOP = 0x02

    def __init__(self):
        super(CommonBaseEngine, self).__init__()
        self.name = "BaseEngine"
        self.status = self.EngineStatus.READY
        self.ev: threading.Event = threading.Event()
        self.thread: typing.Optional[threading.Thread] = None
        self.thread_pool: typing.Optional[list[threading.Thread]] = None

    def is_running_status(self):
        """
        判断引擎是否处于运行状态
        :return:
        """
        return self.status == self.EngineStatus.RUNNING

    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def stop(self, force=True):
        pass

    @abc.abstractmethod
    def is_thread_alive(self):
        """
        判断引擎的线程是否处于运行状态
        :return:
        """
        pass

    @abc.abstractmethod
    def _worker(self):
        pass


class SingleThreadEngine(CommonBaseEngine):
    """
    基于单线程的引擎模块
    """

    def __init__(self, name="SingleThreadEngine"):
        super(SingleThreadEngine, self).__init__()
        self.name = name

    def start(self):
        self.status = self.EngineStatus.RUNNING
        self.thread: threading.Thread = threading.Thread(target=self._worker, name=self.name)
        self.thread.start()

    def stop(self, force=True):
        self.status = self.EngineStatus.STOP
        self.ev.set()

    def is_thread_alive(self):
        return self.thread.is_alive()

    @abc.abstractmethod
    def _worker(self):
        pass


class MultiThreadEngine(CommonBaseEngine):
    """
    基于多线程的基础引擎
    """

    def __init__(self, name="MultiThreadEngine", pool_size=None):
        super(MultiThreadEngine, self).__init__()
        self.name = name
        self.pool_size = pool_size if pool_size else multiprocessing.cpu_count() * 2 + 1

    def start(self):
        self.status = self.EngineStatus.RUNNING
        self.thread_pool = \
            [threading.Thread(
                target=self._worker, name="{}-{}".format(self.name, idx)
            ) for idx in range(self.pool_size)]
        _ = [t.start() for t in self.thread_pool]

    def stop(self, force=True):
        self.status = self.EngineStatus.STOP
        self.ev.set()

    def is_thread_alive(self):
        return any([t.is_alive() for t in self.thread_pool])

    @abc.abstractmethod
    def _worker(self):
        pass
