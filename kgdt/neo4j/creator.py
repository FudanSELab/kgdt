from py2neo import Node


class NodeBuilder:
    """
    a builder for Node
    """

    def __init__(self):
        self.labels = []
        self.property_dict = {}

    def add_labels(self, *labels):
        """
        add labels for Node
        :param labels: all labels need to be added
        :return: a NodeBuilder object
        """
        self.labels.extend(labels)
        self.labels = list(set(self.labels))

        return self

    def add_label(self, label):
        """
        add a label for Node
        :param label: label, string
        :return: a NodeBuilder object
        """
        if label not in self.labels:
            self.labels.append(label)
        return self

    def add_property(self, **property_dict):
        for property_name, property_value in property_dict.items():
            self.add_one_property(property_name, property_value)
        return self

    def add_one_property(self, property_name, property_value):
        """
        add one property to the Node for building
        :param property_name: the name of property, the name must be not empty string and not None
        :param property_value: the value of property , must be not empty string and not None
        :return:
        """
        if property_name is None or property_name is "":
            return self
        if property_value is None:
            return self
        if type(property_value) == set:
            self.property_dict[property_name] = list(property_value)
        else:
            self.property_dict[property_name] = property_value
        return self

    def add_entity_label(self):
        return self.add_labels('entity')

    def build(self):
        node = Node(*self.labels)
        for key in self.property_dict:
            node[key] = self.property_dict[key]
        return node

    def get_labels(self):
        """
        get the labels for current built node
        :return: a set of labels
        """
        return self.labels

    def get_properties(self):
        """
        get the properties for current built node
        :return: a dict of properties
        """
        return self.property_dict

    def build_node_json(self):
        ##todo: change the value to constant
        node_json = {"id": -1, "properties": self.property_dict, "labels": self.labels}
        return node_json
