#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-----------------------------------------
@Author: isky
@Email: 19110240019@fudan.edu.cn
@Created: 2019/10/16
------------------------------------------
@Modify: 2019/10/16
------------------------------------------
@Description:
"""
from unittest import TestCase

from kgdt.models.graph import GraphData
from kgdt.retrieval.property.str_property_retrieval import StrPropertySearcher


class TestStrPropertySearcher(TestCase):
    def get_graph(self):
        graph_data = GraphData()

        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.add()", "name": "ArrayList.add",
                                         "alias": ["ArrayList.add1", "add()", "add"]})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.add(int)", "name": "ArrayList.add",
                                         "alias": ["ArrayList.add2", "add()", "add"]})

        graph_data.add_node({"override method"}, {"qualified_name": "ArrayList.pop"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.remove"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.clear"})

        return graph_data

    def test_start_training(self):
        graph_data = self.get_graph()
        searcher: StrPropertySearcher = StrPropertySearcher.train(graph_data, "name", "qualified_name", "alias")

        self.assertEqual(set([1]), searcher.search_by_value_exactly("ArrayList.add()"))
        self.assertEqual(set([2]), searcher.search_by_value_exactly("ArrayList.add(int)"))
        self.assertEqual(set([1, 2]), searcher.search_by_value_exactly("ArrayList.add"))
        self.assertEqual(set([2]), searcher.search_by_value_exactly("ArrayList.add2"))
