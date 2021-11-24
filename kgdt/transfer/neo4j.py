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

from kgdt.models.graph import GraphData
from kgdt.neo4j.accessor.base import GraphAccessor
from kgdt.neo4j.accessor.index import IndexGraphAccessor
from kgdt.neo4j.accessor.metadata import MetadataGraphAccessor
from kgdt.neo4j.creator import NodeBuilder
from kgdt.utils import catch_exception
import os

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
        index_accessor = IndexGraphAccessor(self.graph_accessor)
        index_accessor.create_index(label=self.DEFAULT_LABEL, property_name=self.DEFAULT_PRIMARY_KEY)

        if clear:
            self.graph_accessor.delete_all_relations()
            self.graph_accessor.delete_all_nodes()

        # todo: this is slow, need to speed up, maybe not commit on every step
        all_node_ids = graph_data.get_node_ids()
        for node_id in all_node_ids:
            ## todo: fix this by not using 'properties','labels'
            node_info_dict = graph_data.get_node_info_dict(node_id)
            properties = node_info_dict['properties']
            labels = node_info_dict['labels']
            self.import_one_entity(node_id, properties, labels)

        print("all entity imported")
        relations = graph_data.get_relations()
        for r in relations:
            start_node_id, r_name, end_node_id = r
            start_node = self.graph_accessor.find_node(primary_label=self.DEFAULT_LABEL,
                                                       primary_property=self.DEFAULT_PRIMARY_KEY,
                                                       primary_property_value=start_node_id)
            end_node = self.graph_accessor.find_node(primary_label=self.DEFAULT_LABEL,
                                                     primary_property=self.DEFAULT_PRIMARY_KEY,
                                                     primary_property_value=end_node_id)

            if start_node is not None and end_node is not None:
                try:
                    self.graph_accessor.create_relation_without_duplicate(start_node, r_name, end_node)
                except Exception as e:
                    traceback.print_exc()
            else:
                print("fail create relation because start node or end node is none.")
        print("all relation imported")

        print("all graph data import finish")

    @catch_exception
    def import_one_entity(self, node_id, property_dict, labels):
        builder = NodeBuilder()
        builder.add_label(self.DEFAULT_LABEL).add_property(**property_dict). \
            add_one_property(property_name=self.DEFAULT_PRIMARY_KEY, property_value=node_id).add_labels(*labels)
        node = builder.build()

        node = self.graph_accessor.create_or_update_node(node, primary_label=self.DEFAULT_LABEL,
                                                         primary_property=self.DEFAULT_PRIMARY_KEY)


class BatchNeo4jImporter:
    '''
    The class is used to import datas from csv to a Neo4j database.
    '''

    def __init__(self, graph_client: GraphAccessor):
        self.graph_accessor = graph_client

    def batch_import_nodes_from_csv(self, commit_num, csv_file, labels, property_name_in_neo4j_to_property_name_in_csv):
        '''
        就是为了生成以下格式的cypher语句
        using periodic commit 1000 load csv with headers from "file:///good.csv" as line
        merge(no:`good`:`entity`{`good id`: line["good id"] ,`good name`: line["good name"] })
        :param commit_num: how many nodes are submitted once during batch import
        :param csv_file: batch imported CSV file name, the CSV file should be placed in the import folder
        :param labels: a set ,the node labels
        :param property_name_in_neo4j_to_property_name_in_csv: a dict
        :return:
        '''
        cypher_start = 'using periodic commit {} load csv with headers from "file:///{}" as line merge(no'.format(
            commit_num, csv_file)
        cypher = cypher_start
        for label in labels:
            cypher_label = ':`{}`'.format(label)
            cypher = cypher + cypher_label
        cypher = cypher + '{'
        for property_name_in_neo4j, property_name_in_csv in property_name_in_neo4j_to_property_name_in_csv.items():
            cypher_property = '`{}`: line["{}"] ,'.format(property_name_in_neo4j, property_name_in_csv)
            cypher = cypher + cypher_property
        if cypher[-1] == ',':
            cypher = cypher[:-1]
        cypher_end = '})'
        cypher = cypher + cypher_end
        return self.graph_accessor.graph.run(cypher)

    def batch_import_relations_from_csv(self, commit_num, csv_file, match_nodes, relations):
        '''
        就是为了生成以下格式的cypher语句
        using periodic commit 1000 load csv with headers from "file:///rela.csv" as line
        match(p1:`person`:`entity`{`person id`:line["person id"]}),(p2:`good`:`entity`{`good id`:line["good id"]})
        merge (p1)-[:`buy`]->(p2)

        :param commit_num: how many nodes are submitted once during batch import
        :param csv_file: batch imported CSV file name, the CSV file should be placed in the import folder
        :param match_nodes: 一个 match_node的列表，每个match_node又是一个长度为三的列表，example:
                        match_nodes = [[('person', 'entity'), 'person id', 'person id'],
                                        [('good', 'entity'), 'good id', 'good id']]
        :param relations: 一个关系的列表,每个关系又是长度为3的列表, example:
                        relations = [[1, 'buy', 2] 代表前面第一个match_node buy 第二个match_node
        :return:
        '''
        cypher_start = 'using periodic commit {} load csv with headers from "file:///{}" as line '.format(commit_num,
                                                                                                csv_file)
        cypher = cypher_start
        index = 1
        cypher_match = 'match'
        cypher = cypher + cypher_match
        for match_node in match_nodes:
            cypher_match_start = '(p{}'.format(str(index))
            cypher_match = cypher_match_start
            for match_node_ in match_node[0]:
                cypher_match_label = ':`{}`'.format(match_node_)
                cypher_match = cypher_match + cypher_match_label
            cypher_match_end = '{{`{}`:line["{}"]}}),'.format(match_node[1], match_node[2])
            cypher_match = cypher_match + cypher_match_end
            cypher = cypher + cypher_match
            index = index + 1
        if cypher[-1] == ',':
            cypher = cypher[:-1]
        for relation in relations:
            cypher_merge = ' merge (p{})-[:`{}`]->(p{})'.format(relation[0], relation[1], relation[2])
            cypher = cypher + cypher_merge
        return self.graph_accessor.graph.run(cypher)

    def batch_import_nodes_by_neo4j_admin(self, neo4j_admin_location, database_name, csv_file_2_labels):
        '''
        就是为了生成以下格式的命令
        D:\neo4j\Soft\neo4j-community-4.3.5\bin\neo4j-admin import
        --database=mydatabase --id-type=STRING
        --nodes="person":"entity"=D:\pycharm\Code\libkg\data\person.csv
        --nodes="good":"entity"=D:\pycharm\Code\libkg\data\good.csv
        --ignore-extra-columns=True --multiline-fields=True

        :param neo4j_admin_location: the location of neo4j_admin
        :param database_name: the name of the neo4j database you want to generate
        :param csv_file_2_labels: a dict
        :return:
        '''
        commend_start = neo4j_admin_location + ' import --database={} --id-type=STRING'.format(database_name)
        commend = commend_start
        for csv_file, labels in csv_file_2_labels.items():
            commend_node_start = ' --nodes='
            commend_node = commend_node_start
            for label in labels:
                commend_node_label = '"{}":'.format(label)
                commend_node = commend_node + commend_node_label
            if commend_node[-1] == ':':
                commend_node = commend_node[:-1]
            commend_node_end = '={}'.format(csv_file)
            commend_node = commend_node + commend_node_end
            commend = commend + commend_node
        commend_end = ' --ignore-extra-columns=True --multiline-fields=True'
        commend = commend + commend_end
        print(commend)
        return self.excute_command(commend)

    def excute_command(self, command):
        '''
        :param command: the command to execute
        :return: o is success , 1 is defeat
        '''
        return os.system(command)



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
