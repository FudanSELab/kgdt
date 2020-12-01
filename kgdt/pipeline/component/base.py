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
@Description: This is the basic class for pipeline component. The specific project could design
their components for specific goal, e.g., build a API KG from jar file.
"""
from abc import abstractmethod

from kgdt.models.doc import MultiFieldDocumentCollection
from kgdt.models.graph import GraphData


class ComponentListener:

    @abstractmethod
    def on_before(self, component, **config):
        print("hook before component running")

    @abstractmethod
    def on_after(self, component, **config):
        print("hook after component running")


class Component:
    def __init__(self, graph_data=None, doc_collection=None):
        if graph_data is not None:
            self.graph_data = graph_data
        else:
            self.graph_data = GraphData()
        if doc_collection is not None:
            self.doc_collection = doc_collection
        else:
            self.doc_collection = MultiFieldDocumentCollection()
        self.__before_run_listeners = []
        self.__after_run_listeners = []

    def set_graph_data(self, graph_data):
        self.graph_data = graph_data

    def set_doc_collection(self, doc_collection):
        self.doc_collection = doc_collection

    def type(self):
        return str(self.__class__.__name__)

    @abstractmethod
    def run(self, **config):
        print("running component %r" % (self.type()))

    def add_before_listener(self, listener: ComponentListener):
        self.__before_run_listeners.append(listener)

    def add_after_listener(self, listener: ComponentListener):
        self.__after_run_listeners.append(listener)

    def before_run(self, **config):
        print("before running component %r" % (self.type()))
        for listener in self.__before_run_listeners:
            listener.on_before(self, **config)

    def after_run(self, **config):
        print("after running component %r" % (self.type()))
        for listener in self.__after_run_listeners:
            listener.on_after(self, **config)

    @abstractmethod
    def provided_entities(self):
        """
        get the entity labels provided after this component
        :return: a list of entity labels
        """
        return set()

    @abstractmethod
    def dependent_entities(self):
        """
        get the dependent entities for the running of this component
        :return: a set of str. each str is the name of the component
        """
        return set()

    @abstractmethod
    def provided_relations(self):
        """
        get the relations provided after this component
        :return: a list of relation names
        """
        return set()

    @abstractmethod
    def dependent_relations(self):
        """
        get the dependent relations for the running of this component
        :return: a set of str. each str is the name of the component
        """
        return set()

    @abstractmethod
    def provided_document_fields(self):
        """
        get the document fields provided after this component, e.g., html_description, clean_short_description
        :return: a list of documents fields
        """
        return set()

    @abstractmethod
    def dependent_document_fields(self):
        """
        get the document fields provided after this component, e.g., html_description, clean_short_description
        :return: a list of documents fields
        """
        return set()
