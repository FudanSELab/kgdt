# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-----------------------------------------
@Author: Lenovo
@Email: 21212010059@m.fudan.edu.cn
@Created: 2021/11/08
------------------------------------------
@Modify: 2021/11/08
------------------------------------------
@Description:
"""
from unittest import TestCase
from definitions import ROOT_DIR
import os
import pandas as pd
from kgdt.neo4j.factory import GraphInstanceFactory
from kgdt.neo4j.accessor.base import GraphAccessor
from kgdt.transfer.neo4j import BatchNeo4jImporter
from kgdt.models.graph import GraphData
from kgdt.transfer.neo4j import CSVGraphdataTranformer


class TestBatchNeo4jImporter(TestCase):

    def setUp(self):
        """Set up test fixtures, if any."""
        project_header = 'project ID,project platform,project name,created timestamp,updated timestamp,description,keywords,homepage URL,licenses,repository URL,project versions count,sourcerank,latest release publish timestamp,latest release number,package manager ID,dependent projects count,language,status,last synced timestamp,dependent repositories count,repository ID'.split(
            ',')
        project_version_header = 'project version ID,project version platform,project name,project ID,project version number,published timestamp,created timestamp,updated timestamp'.split(
            ',')
        project_version_dependency_header = 'project version dependency ID,project version platform,project name,project ID,project version number,project version ID,project version dependency name,project version dependency platform,project version dependency kind,optional dependency,project version dependency requirements,dependency project ID'.split(
            ',')
        data = pd.read_csv(os.path.join(ROOT_DIR, 'data', 'project_test.csv'), dtype=object)
        data.to_csv(os.path.join(r'neo4j-community-4.2.0', 'import', 'project_test.csv'), header=project_header,
                    index=False)
        data = pd.read_csv(os.path.join(ROOT_DIR, 'data', 'project_version_test.csv'), dtype=object)
        data.to_csv(os.path.join(r'neo4j-community-4.2.0', 'import', 'project_version_test.csv'),
                    header=project_version_header, index=False)
        data = pd.read_csv(os.path.join(ROOT_DIR, 'data', 'project_version_dependency_test.csv'), dtype=object)
        data.to_csv(os.path.join(r'neo4j-community-4.2.0', 'import', 'project_version_dependency_test.csv'),
                    header=project_version_dependency_header, index=False)

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_batch_import_from_csv(self):
        graphinstancefactory = GraphInstanceFactory(os.path.join(ROOT_DIR, 'neo4j_config.json'))
        graph = graphinstancefactory.create_py2neo_graph_by_server_id(1)
        graphaccessor = GraphAccessor(graph)
        importor = BatchNeo4jImporter(graphaccessor)
        importor.batch_import_nodes_from_csv(1000, 'project_test.csv', {'project'},
                                             {'project ID': 'project ID', 'project name': 'project name'})
        importor.batch_import_nodes_from_csv(1000, 'project_version_test.csv', {'project version'},
                                             {'project version ID': 'project version ID',
                                              'project name': 'project name', 'project ID': 'project ID'})
        importor.batch_import_nodes_from_csv(1000, 'project_version_dependency_test.csv',
                                             {'project version dependency'},
                                             {'project version dependency ID': 'project version dependency ID',
                                              'project version ID': 'project version ID',
                                              'project version dependency name': 'project version dependency name',
                                              'project ID': 'project ID',
                                              'dependency project ID': 'dependency project ID'})

        match_nodes = [[{'project version'}, 'project version ID', 'project version ID'],
                       [{'project'}, 'project ID', 'project ID']]
        relations = [[2, 'has project version', 1]]
        importor.batch_import_relations_from_csv(1000, 'project_version_test.csv', match_nodes, relations)
        match_nodes = [[{'project'}, 'project ID', 'project ID'],
                       [{'project version'}, 'project version ID', 'project version ID'],
                       [{'project version dependency'}, 'project version dependency ID',
                        'project version dependency ID'],
                       [{'project'}, 'project ID', 'dependency project ID']
                       ]

        relations = [[1, 'has project version dependency', 3],
                     [2, 'has project version dependency', 3],
                     [3, 'depend on project', 4]
                     ]
        importor.batch_import_relations_from_csv(1000, 'project_version_dependency_test.csv', match_nodes, relations)
        nodes = graph.run("match (n) return n")
        print(nodes.data())


class TestCSVGraphdataTranformer(TestCase):


    def test_graphdata2csv(self):
        '''
        这样就将graphdata中的数据导入到我们指定的csv文件夹中，会有多个节点csv文件
        '''
        '''
        datadir = os.path.join(ROOT_DIR, 'data')
        csvdir = os.path.join(ROOT_DIR, 'output')
        graph = GraphData.load(os.path.join(datadir, 'graph.graph'))
        transfer = CSVGraphdataTranformer()
        transfer.graphdata2csv(csvdir, graph)
        '''
        pass


    def test_node_csv2graphdata(self):
        '''
        导入节点csv文件测试
        '''
        '''
        datadir = os.path.join(ROOT_DIR, 'data')
        csvdir = os.path.join(ROOT_DIR, 'output')
        files = os.listdir(csvdir)
        transfer = CSVGraphdataTranformer()
        graph_new = GraphData()
        for file in files:
            if file != 'relations.csv':
                graph_new = transfer.node_csv2graphdata(os.path.join(csvdir, file), graph_new)
        graph_new.save(os.path.join(datadir, 'graph_new.graph'))
        '''
        pass


    def test_relation_csv2graphdata(self):
        '''
        导入关系csv文件测试
        '''
        '''
        datadir = os.path.join(ROOT_DIR, 'data')
        csvdir = os.path.join(ROOT_DIR, 'output')
        files = os.listdir(csvdir)
        transfer = CSVGraphdataTranformer()
        graph_new = GraphData()
        for file in files:
            if file == 'relations.csv':
                graph_new = transfer.node_csv2graphdata(os.path.join(csvdir, file), graph_new)
        graph_new.save(os.path.join(datadir, 'graph_new.graph'))
        '''
        pass
