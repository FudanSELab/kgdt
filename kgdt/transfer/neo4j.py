#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-----------------------------------------
@Author: isky
@Email: 19110240019@fudan.edu.cn
@Created: 2020/12/01
------------------------------------------
@Modify: 2020/12/01
------------------------------------------
@Description:
"""
import traceback

from py2neo import Subgraph

from kgdt.models.graph import GraphData
from kgdt.neo4j.accessor.base import GraphAccessor
from kgdt.neo4j.accessor.index import IndexGraphAccessor
from kgdt.neo4j.accessor.metadata import MetadataGraphAccessor
from kgdt.neo4j.creator import NodeBuilder, RelationshipBuilder
from kgdt.utils import catch_exception


class DataExporterAccessor(GraphAccessor):
    def get_all_nodes_not_batch(self, node_label):
        try:
            query = 'Match (n:`{node_label}`) return n'.format(node_label=node_label)

            cursor = self.graph.run(query)
            nodes = []
            for record in cursor:
                nodes.append(record["n"])
            return nodes
        except Exception:
            return []

    def get_all_nodes(self, node_label, step=100000):
        metadata_accessor = MetadataGraphAccessor(self)
        max_node_id = metadata_accessor.get_max_id_for_node()

        nodes = []

        for start_id in range(0, max_node_id, step):
            end_id = min(max_node_id, start_id + step)
            nodes.extend(self.get_all_nodes_in_scope(node_label, start_id=start_id, end_id=end_id))
            print("get nodes step %d-%d" % (start_id, end_id))

        return nodes

    def get_all_nodes_in_scope(self, node_label, start_id, end_id):
        try:
            query = 'Match (n:`{node_label}`) where ID(n)>{start_id} and ID(n)<={end_id} return n'.format(
                node_label=node_label, start_id=start_id, end_id=end_id)

            cursor = self.graph.run(query)
            nodes = []
            for record in cursor:
                nodes.append(record["n"])

            return nodes
        except Exception:
            return []

    def get_all_relation(self, node_label, step=200000):
        metadata_accessor = MetadataGraphAccessor(self)
        max_relation_id = metadata_accessor.get_max_id_for_relation()

        relations = []

        for start_id in range(0, max_relation_id, step):
            end_id = min(max_relation_id, start_id + step)
            relations.extend(self.get_all_relation_in_scope(node_label, start_id=start_id, end_id=end_id))
            print("get relation step %d-%d" % (start_id, end_id))
        return relations

    def get_all_relation_in_scope(self, node_label, start_id, end_id):
        try:
            query = 'Match (n:`{node_label}`)-[r]->(m:`{node_label}`) where ID(r)>{start_id} and ID(r)<={end_id} return ID(n) as startId,ID(m) as endId, type(r) as relationType'.format(
                node_label=node_label, start_id=start_id, end_id=end_id)

            cursor = self.graph.run(query)
            data = cursor.data()
            return data
        except Exception:
            return []

    def get_all_relation_not_batch(self, node_label):
        try:
            query = 'Match (n:`{node_label}`)-[r]->(m:`{node_label}`) return ID(n) as startId,ID(m) as endId, type(r) as relationType'.format(
                node_label=node_label)

            cursor = self.graph.run(query)
            data = cursor.data()
            return data
        except Exception:
            return []


class Neo4jImporter:
    """
    The class is used to import one GraphData obj to a Neo4j database.
    """
    DEFAULT_LABEL = "entity"
    DEFAULT_PRIMARY_KEY = "_node_id"

    def __init__(self, graph_client: GraphAccessor):
        self.graph_accessor = graph_client

    def import_all_graph_data(self, graph_data: GraphData, clear=True):
        """
        import all data in one GraphData into neo4j and create index on node
        :param graph_data:
        :param clear: clear the graph content, default is not clear the graph contain
        :return:
        """

        # todo: this statement may cause some warnings, maybe need to fix
        index_accessor = IndexGraphAccessor(self.graph_accessor)
        # index_accessor.create_index(label=self.DEFAULT_LABEL, property_name=self.DEFAULT_PRIMARY_KEY)

        if clear:
            self.graph_accessor.delete_all_relations()
            self.graph_accessor.delete_all_nodes()

        nodes = []
        node_dict = {}

        all_node_ids = graph_data.get_node_ids()
        for node_id in all_node_ids:
            node_info_dict = graph_data.get_node_info_dict(node_id)
            properties = node_info_dict['properties']
            labels = node_info_dict['labels']
            node = self.create_one_node(node_id, properties, labels)
            node_dict[node_id] = node
            nodes.append(node)
        print("all nodes created")

        all_relations = graph_data.get_relations()
        relations = []
        for r in all_relations:
            start_node_id, r_name, end_node_id = r
            start_node = node_dict[start_node_id]
            end_node = node_dict[end_node_id]

            if start_node is not None and end_node is not None:
                try:
                    relation = self.create_one_relationship(start_node, r_name, end_node)
                    relations.append(relation)
                except Exception as e:
                    traceback.print_exc()
            else:
                print("fail create relation because start node or end node is none.")
        print("all relations created")
        self.graph_accessor.create_all_nodes_and_relations(nodes, relations)
        print("graph data created")

        print("all graph data import finish")

    @catch_exception
    def create_one_node(self, node_id, property_dict, labels):
        builder = NodeBuilder()
        builder.add_label(self.DEFAULT_LABEL).add_property(**property_dict). \
            add_one_property(property_name=self.DEFAULT_PRIMARY_KEY, property_value=node_id).add_labels(*labels)
        node = builder.build()
        return node

    @catch_exception
    def create_one_relationship(self, start_node, r_name, end_node):
        builder = RelationshipBuilder()
        builder.set_start_node(start_node).set_end_node(end_node).set_name(r_name)
        relation = builder.build()
        return relation


class GraphDataExporter:
    """
    export specific data
    """

    ## todo: this class should be a opponent class Neo4jImporter, it export Neo4j data as a GraphData obj
    ## todo: take care of the "node_id" problem,
    def __init__(self):
        pass

    def export_all_graph_data(self, graph, node_label):
        accessor = DataExporterAccessor(graph=graph)
        nodes = accessor.get_all_nodes(node_label=node_label)
        graph_data = GraphData()

        for node in nodes:
            labels = [label for label in node.labels]
            graph_data.add_node(node_id=node.identity, node_labels=labels, node_properties=dict(node))

        print("load entity complete, num=%d" % len(nodes))
        relations = accessor.get_all_relation(node_label=node_label)
        print("load relation complete,num=%d" % len(relations))
        graph_data.set_relations(relations=relations)

        return graph_data
