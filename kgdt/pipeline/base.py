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
@Description: The definition of the base KG Build Pipeline
"""
from abc import abstractmethod

from kgdt.models.doc import MultiFieldDocumentCollection
from kgdt.models.graph import GraphData
from kgdt.pipeline.component.base import Component


class ComponentOrderError(Exception):
    pass


class MissingComponentError(Exception):
    pass


class ComponentNotExistError(Exception):
    def __init__(self, component_name):
        super().__init__("The component named %s doesn't exist" % component_name)
        self.message = component_name


class ComponentDependencyError(Exception):
    def __init__(self, component_name, missing_dependency_infos):
        super().__init__(
            "The component named %s has dependency error with %r" % (component_name, missing_dependency_infos))
        self.message = component_name
        self.missing_dependency_infos = missing_dependency_infos


class PipelineListener:

    @abstractmethod
    def on_before_run_component(self, component_name, kg_build_pipeline, **config):
        print("hook before pipeline run component %r" % component_name)

    @abstractmethod
    def on_after_run_component(self, component_name, kg_build_pipeline, **config):
        print("hook after pipeline run component %r" % component_name)


class KGBuildPipeline:
    def __init__(self):
        self.__name2component = {}
        self.__component_order = []
        self.__graph_data = GraphData()
        self.__doc_collection = MultiFieldDocumentCollection()
        self.__before_run_component_listeners = {}
        self.__after_run_component_listeners = {}

    def __repr__(self):
        return str(self.__component_order)

    def exist_component(self, component_name):
        """
        check whether the component exist in the Pipeline
        :param component_name: the name of component
        :return: True, exist, False, not exist.
        """
        if component_name in self.__component_order:
            return True
        return False

    def add_before_listener(self, component_name, listener: PipelineListener):
        """
        add a new PipelineListener running before a specific component
        :param component_name: the name of the component
        :param listener: the PipelineListener
        :return:
        """
        if not self.exist_component(component_name):
            raise ComponentNotExistError(component_name)

        if component_name not in self.__before_run_component_listeners:
            self.__before_run_component_listeners[component_name] = []
        self.__before_run_component_listeners[component_name].append(listener)

    def add_after_listener(self, component_name, listener: PipelineListener):
        if component_name not in self.__after_run_component_listeners:
            self.__after_run_component_listeners[component_name] = []
        self.__after_run_component_listeners[component_name].append(listener)

    def __get_component_order(self, name):
        """
        get the order of the specific component
        :param name: the specific component
        :return: the order start from 0 to num(component), -1. the specific component not exist
        """
        self.__component_order.append(name)

        for order, exist_component in enumerate(self.__component_order):
            if exist_component == name:
                return order
        return -1

    def __allocate_order_for_new_component(self, before=None, after=None):
        """
        try to allocate the right position for the new component
        :param before: the component of this new component must run before
        :param after: the component of this new component must run after
        :return: -1, can't not find a right order.
        """
        min_order = 0
        max_order = self.num_of_components()

        if before is not None:
            max_order = self.__get_component_order(before)
            if max_order == -1:
                max_order = self.num_of_components()

        if after is not None:
            min_order = self.__get_component_order(after) + 1
            if min_order == -1:
                min_order = 0
        if min_order > max_order:
            return -1
        return max_order

    def add_component(self, name, component: Component, before=None, after=None, **config):
        """
        add a new component to this pipeline with given name. In a pipeline, the component name must be unique.
        :param after: the component name the this new component must run after
        :param before: the component name the this new component must run after
        :param name: the name of this new component
        :param component: the component instance
        :param config: the other config, save for update
        :return:
        """

        order = self.__allocate_order_for_new_component(before=before, after=after)
        if order == -1:
            raise ComponentOrderError("Can't not find a right order for %s" % name)

        component.set_graph_data(self.__graph_data)
        component.set_doc_collection(self.__doc_collection)
        self.__name2component[name] = component

        self.__component_order.insert(order, name)

    def check(self):
        """
        check whether the components in the pipeline setting correct.
        e.g., the order of the component is wrong.
        the necessary component for a component to run is missing.
        :return: True the pipeline is correct.
        """
        current_entities = self.get_provided_entities()
        current_relations = self.get_provided_relations()
        current_document_fields = self.get_provided_document_fields()

        component_pairs = self.get_component_name_with_component_pair_by_order()

        for component_name, component in component_pairs:
            missing_entities = component.dependent_entities() - current_entities
            if missing_entities != set():
                raise ComponentDependencyError(component_name, missing_entities)
            current_entities.update(component.provided_entities())

            missing_relations = component.dependent_relations() - current_relations
            if missing_entities != set():
                raise ComponentDependencyError(component_name, missing_relations)
            current_relations.update(component.provided_relations())

            missing_fields = component.dependent_document_fields() - current_document_fields
            if missing_fields != set():
                raise ComponentDependencyError(component_name, missing_fields)

            current_document_fields.update(component.provided_document_fields())

        return True

    def get_provided_document_fields(self):
        """
        get the provided entity type set for the pipeline from the current DocumentCollection. If the pipeline start from empty state,
        This method will return empty set
        :return:
        """
        return set(self.__doc_collection.get_field_set())

    def get_provided_relations(self):
        """
        get the provided relation type set for the pipeline from the current GraphData. If the pipeline start from empty state,
        This method will return empty set
        :return:
        """
        return self.__graph_data.get_all_relation_types()

    def get_provided_entities(self):
        """
        get the provided entity type set for the pipeline from the current GraphData. If the pipeline start from empty state,
        This method will return empty set
        :return:
        """
        return set(self.__graph_data.get_all_labels())

    def get_components_by_order(self):
        components = []
        for component_name in self.__component_order:
            component: Component = self.__name2component[component_name]
            components.append(component)
        return components

    def get_component_name_with_component_pair_by_order(self):
        components = []
        for component_name in self.__component_order:
            component: Component = self.__name2component[component_name]
            components.append((component_name, component))
        return components

    def run(self, **config):
        self.check()
        print("start running the pipeline")
        for component_name in self.__component_order:
            component: Component = self.__name2component[component_name]
            self.before_run_component(component_name, **config)
            component.before_run()
            component.run()
            component.after_run()
            self.after_run_component(component_name, **config)

        print("finish running the pipeline")

    def before_run_component(self, component_name, **config):
        print("start running with name=%r in the pipeline" % component_name)
        for listener in self.__before_run_component_listeners.get(component_name, []):
            listener.on_before_run_component(component_name, self, **config)

    def after_run_component(self, component_name, **config):
        print("finish running with name=%r in the pipeline\n" % component_name)
        for listener in self.__after_run_component_listeners.get(component_name, []):
            listener.on_after_run_component(component_name, self, **config)

    def save(self, graph_path=None, doc_path=None):
        """
        save the graph data object after all the building of all component
        :param doc_path: the path to save the DocumentCollection
        :param graph_path: the path to save the GraphData
        :return:
        """
        self.save_graph(path=graph_path)
        self.save_doc(path=doc_path)

    def save_graph(self, path):
        if path is None:
            return
        self.__graph_data.save(path)

    def save_doc(self, path):
        if path is None:
            return
        self.__doc_collection.save(path)

    def load_graph(self, graph_data_path):
        self.__graph_data = GraphData.load(graph_data_path)
        # update component graph data
        for component_name in self.__component_order:
            component: Component = self.__name2component[component_name]
            component.set_graph_data(self.__graph_data)

        print("load graph")

    def load_doc(self, document_collection_path):
        self.__doc_collection = MultiFieldDocumentCollection.load(document_collection_path)
        # update component doc_collection
        for component_name in self.__component_order:
            component: Component = self.__name2component[component_name]
            component.set_doc_collection(self.__doc_collection)

        print("load doc collection")

    def num_of_components(self):
        return len(self.__component_order)
