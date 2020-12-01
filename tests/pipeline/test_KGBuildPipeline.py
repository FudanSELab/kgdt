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
from unittest import TestCase

from kgdt.pipeline.base import KGBuildPipeline, PipelineListener
from kgdt.pipeline.component.base import ComponentListener
from kgdt.pipeline.component.example import EmptyComponent


class TestKGBuildPipeline(TestCase):

    def test_run(self):
        pipeline = KGBuildPipeline()
        component1 = EmptyComponent()
        pipeline.add_component("hello1", component1)
        component2 = EmptyComponent()
        pipeline.add_component("hello2", component2)

        component1.add_before_listener(ComponentListener())

        pipeline.add_after_listener("hello1", PipelineListener())
        pipeline.add_before_listener("hello2", PipelineListener())

        pipeline.run()
