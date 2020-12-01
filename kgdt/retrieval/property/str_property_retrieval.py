from pathlib import Path

from kgdt.models.graph import GraphData
from kgdt.utils import SaveLoad


class StrPropertySearcher(SaveLoad):
    """
    Search some node in KG on some properties by value. The type of the property value must be
    str or list/set of str.
    Call train()/start_training() and specify the name properties need to search on.
    """

    def __init__(self):
        self.value_2_ids_map = {}
        self.id_2_values_map = {}
        self.value_keyword_2_ids_map = {}
        self.id_2_value_keywords_map = {}

    @classmethod
    def train(cls, graph_data: GraphData or str or Path, *properties):
        """
        train the kg name searcher model from a graph data object by specifying name properties.
        :param properties: the properties that need to be searched on, could be more than one. e.g., "name","qualified_name","labels_en"
        :param graph_data:the path of graph data.
        :return:
        """
        # todo: add some config arguments, to control whether lower the case, split the words.
        if graph_data == None:
            raise Exception("Input GraphData object not exist")

        graph_data_source = None
        if type(graph_data) == str:
            graph_data_source: GraphData = GraphData.load(graph_data)
        if type(graph_data) == Path:
            graph_data_source: GraphData = GraphData.load(str(graph_data))
        if type(graph_data) == GraphData:
            graph_data_source = graph_data

        if graph_data_source is None:
            raise Exception("can't find the graph data")

        searcher = cls()
        searcher.start_training(graph_data_source, *properties)
        return searcher

    def start_training(self, graph_data: GraphData, *properties):
        """
        start train the kg name searcher model from a graph data object by specifying name properties.
        :param properties: the properties that need to be searched on. e.g., "name","qualified_name","labels_en"
        :param graph_data: the GraphData instance
        :return:
        """
        # todo: add some config arguments, to control whether lower the case, split the words.
        self.clear()
        for node_id in graph_data.get_node_ids():
            node_properties = graph_data.get_properties_for_node(node_id=node_id)
            for property_name in properties:
                property_value = node_properties.get(property_name, None)
                if not property_value:
                    continue
                if type(property_value) == list or type(property_value) == set:
                    iterable_property_values = property_value
                    for single_value in iterable_property_values:
                        self.add_from_property_value(single_value, node_id)

                else:
                    single_value = property_value
                    self.add_from_property_value(single_value, node_id)

    def cache_single_property_value(self, value, id):
        if not value:
            return
        if value not in self.value_2_ids_map.keys():
            self.value_2_ids_map[value] = set([])
        if id not in self.id_2_values_map.keys():
            self.id_2_values_map[id] = set([])

        self.value_2_ids_map[value].add(id)
        self.id_2_values_map[id].add(value)

    def add_keyword_for_id(self, keyword, id):
        if not keyword:
            return
        if keyword not in self.value_keyword_2_ids_map.keys():
            self.value_keyword_2_ids_map[keyword] = set([])
        if id not in self.id_2_value_keywords_map.keys():
            self.id_2_value_keywords_map[id] = set([])

        self.value_keyword_2_ids_map[keyword].add(id)

        self.id_2_value_keywords_map[id].add(keyword)

    def add_keyword_map_from_full_name(self, full_name, id):
        name_words = self.generate_value_keywords(full_name)
        for word in name_words:
            self.add_keyword_for_id(word, id)
            self.add_keyword_for_id(word.lower(), id)

    @staticmethod
    def generate_value_keywords(value):
        """
        generate the keywords from a given property value
        :param value: a str, representing a property value, e.g., "string buffer"
        :return: a set of str
        """
        value = value.split("(")[0]
        value = value.replace("-", " ").replace("(", " ").replace(")", " ").strip()
        name_words = value.split(" ")

        return set(name_words + [value])

    def add_from_property_value(self, value, node_id):
        """
        add all the cache for search on the name. e.g., the lower name, the separate keywords for keywords search.
        :param value: the name value.
        :param node_id: the node with the name
        :return:
        """
        # todo: lower the value many times. should only do it once
        self.cache_single_property_value(value, node_id)
        self.cache_single_property_value(value.lower(), node_id)
        self.add_keyword_map_from_full_name(value, node_id)
        self.add_keyword_map_from_full_name(value.lower(), node_id)

    def search_by_value_exactly(self, full_name):
        if full_name in self.value_2_ids_map.keys():
            return self.value_2_ids_map[full_name]
        full_name = full_name.lower()
        if full_name in self.value_2_ids_map.keys():
            return self.value_2_ids_map[full_name]
        return set([])

    def search_by_keyword(self, word):
        if word in self.value_keyword_2_ids_map.keys():
            return self.value_keyword_2_ids_map[word]
        word = word.lower()

        if word in self.value_keyword_2_ids_map.keys():
            return self.value_keyword_2_ids_map[word]

        return set([])

    def get_full_names(self, id):
        if id not in self.id_2_values_map:
            return set([])
        else:
            return self.id_2_values_map[id]

    def clear(self):
        self.value_2_ids_map = {}
        self.id_2_values_map = {}
        self.value_keyword_2_ids_map = {}
        self.id_2_value_keywords_map = {}
