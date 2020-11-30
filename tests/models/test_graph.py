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

from kgdt.models.graph import GraphData


class TestGraphData(TestCase):

    def test_get_graph(self):
        graph_data = GraphData()

        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.add"})
        graph_data.add_node({"override method"}, {"qualified_name": "ArrayList.pop"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.remove"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.clear"})

        print(graph_data.get_node_ids())
        print(graph_data.get_relation_pairs_with_type())

        graph_data.add_relation(1, "related to", 2)
        graph_data.add_relation(1, "related to", 3)
        graph_data.add_relation(1, "related to", 4)
        graph_data.add_relation(2, "related to", 3)
        graph_data.add_relation(3, "related to", 4)

        print(graph_data.get_relations(1, "related to"))
        print("get relation by type")
        print(graph_data.get_relations(relation_type="related to"))

        # print(graph_data.get_node_ids())
        # print(graph_data.get_relation_pairs_with_type())

        # print("#" * 50)
        # graph_data.merge_two_nodes_by_id(1, 2)

        # print(graph_data.get_node_ids())
        # print(graph_data.get_relation_pairs_with_type())

    def test_get_graph_with_property(self):
        graph_data = GraphData()

        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.add"})
        graph_data.add_node({"override method"}, {"qualified_name": "ArrayList.pop"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.remove"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.clear"})

        # print(graph_data.get_node_ids())
        # print(graph_data.get_relation_pairs_with_type())

        graph_data.add_relation_with_property(1, "related to", 2, extra_info_key="as")
        graph_data.add_relation_with_property(1, "related to", 3, extra_info_key="ab")
        graph_data.add_relation_with_property(1, "related to", 4, extra_info_key="cs")
        graph_data.add_relation_with_property(2, "related to", 3, extra_info_key="ca")
        graph_data.add_relation_with_property(3, "related to", 4)

        print(graph_data.get_relations(1, "related to"))
        print("get relation by type")
        print(graph_data.get_relations(relation_type="related to"))
        t = graph_data.get_edge_extra_info(1, 2, "related to", extra_key="extra_info_key")
        print(t)
        # print(graph_data.get_node_ids())
        # print(graph_data.get_relation_pairs_with_type())

        # print("#" * 50)
        # graph_data.merge_two_nodes_by_id(1, 2)

        # print(graph_data.get_node_ids())
        # print(graph_data.get_relation_pairs_with_type())

    def test_merge(self):
        graph_data = GraphData()
        graph_data.create_index_on_property("qualified_name", "alias")

        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.add"})
        graph_data.add_node({"override method"}, {"qualified_name": "ArrayList.pop"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.remove"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.clear", "alias": ["clear"]})

        graph_data.merge_node(node_labels=["method", "merge"], node_properties={"qualified_name": "ArrayList.clear",
                                                                                "alias": ["clear", "clear1"]
                                                                                },
                              primary_property_name="qualified_name")

    def test_find_nodes_by_properties(self):
        graph_data = GraphData()
        graph_data.create_index_on_property("qualified_name", "alias")

        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.add"})
        graph_data.add_node({"override method"}, {"qualified_name": "ArrayList.pop"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.remove"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.clear", "alias": ["clear"]})
        graph_data.add_node({"method"},
                            {"qualified_name": "List.clear", "alias": ["clear", "List.clear", "List clear"]})

        match_nodes = graph_data.find_nodes_by_property(property_name="qualified_name", property_value="List.clear")
        print(match_nodes)
        self.assertIsNotNone(match_nodes)
        self.assertEqual(len(match_nodes), 1)
        self.assertEqual(match_nodes[0][GraphData.DEFAULT_KEY_NODE_ID], 5)

        match_nodes = graph_data.find_nodes_by_property(property_name="alias", property_value="clear")

        print(match_nodes)
        self.assertIsNotNone(match_nodes)
        self.assertEqual(len(match_nodes), 2)

    def test_remove_node(self):
        graph_data = GraphData()
        graph_data.create_index_on_property("qualified_name", "alias")

        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.add"})
        graph_data.add_node({"override method"}, {"qualified_name": "ArrayList.pop"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.remove"})
        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.clear", "alias": ["clear"]})
        graph_data.add_node({"method"},
                            {"qualified_name": "List.clear", "alias": ["clear", "List.clear", "List clear"]})
        graph_data.add_relation(1, "related to", 2)
        graph_data.add_relation(1, "related to", 3)
        graph_data.add_relation(1, "related to", 4)
        graph_data.add_relation(2, "related to", 3)
        graph_data.add_relation(3, "related to", 4)

        result = graph_data.remove_node(node_id=1)
        self.assertIsNotNone(result)

        self.assertIsNone(graph_data.get_node_info_dict(node_id=1))

    def test_save_and_load(self):
        graph_data = GraphData()
        graph_data.create_index_on_property("qualified_name", "alias")

        graph_data.add_node({"method"}, {"qualified_name": "ArrayList.add"})
        graph_data.add_node({"override method"}, {"qualified_name": "ArrayList.pop"})

        graph_data.save("test.graph")
        graph_data: GraphData = GraphData.load("test.graph")
        self.assertEqual(graph_data.get_node_num(), 2)

