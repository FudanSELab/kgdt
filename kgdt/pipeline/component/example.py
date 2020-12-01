#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-----------------------------------------
@Author: isky
@Email: 19110240019@fudan.edu.cn
@Created: 2019/10/28
------------------------------------------
@Modify: 2019/10/28
------------------------------------------
@Description:
"""
from abc import abstractmethod

from kgdt.pipeline.component.base import Component


class EmptyComponent(Component):
    def __init__(self, ):
        super().__init__()

    @abstractmethod
    def run(self, **config):
        print("running component %r" % (self.type()))

    def before_run(self, **config):
        super().before_run(**config)
        print("before running component %r" % (self.type()))
        # todo

    def after_run(self, **config):
        super().before_run(**config)
        print("after running component %r" % (self.type()))
        # todo
