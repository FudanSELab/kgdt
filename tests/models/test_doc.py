#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-----------------------------------------
@Author: isky
@Email: 19110240019@fudan.edu.cn
@Created: 2020/11/30
------------------------------------------
@Modify: 2020/11/30
------------------------------------------
@Description:
"""

from unittest import TestCase

from kgdt.models.doc import MultiFieldDocumentCollection


class TestMultiFieldDocumentCollection(TestCase):

    def test_save_and_load(self):
        dc = MultiFieldDocumentCollection()
        dc.add_document_from_field_values(3, "test doc", code="String s=String('s')")
        dc.save("test.v1.dc")
        dc.pretty_print_by_id(3)
        dc: MultiFieldDocumentCollection = MultiFieldDocumentCollection.load("test.v1.dc")
        self.assertEqual(dc.get_num(), 1)
