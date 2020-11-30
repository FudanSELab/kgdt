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
@Description: some class to store document.
"""

import random

from kgdt.utils import SaveLoad


class MultiFieldDocument(SaveLoad):
    """
    This class is a wrapper for the document with multi field.
    For example, for a wikipedia doc. we can has two field "title","body".
    for a stack overflow question, we could have "question title","tags","question body","accept answer".
    For this purpose, this class is used to wrapper the doc like this has multi field.

    """

    def __init__(self, id, name, **field_to_field_doc_map):
        self.id = id
        self.name = name
        self.field_to_field_doc_map = {}
        self.update_fields(**field_to_field_doc_map)

    def add_field(self, field_name, field_document):
        self.field_to_field_doc_map[field_name] = field_document

    def delete_field(self, field_name):
        self.field_to_field_doc_map.pop(field_name)

    def update_fields(self, **field_to_field_doc_map):
        for field, field_doc in field_to_field_doc_map.items():
            self.field_to_field_doc_map[field] = field_doc

    def get_field_set(self):
        return self.field_to_field_doc_map.keys()

    def get_doc_text_by_field(self, field_name):
        """

        :param field_name:
        :return:
        """
        if field_name in self.field_to_field_doc_map.keys():
            return self.field_to_field_doc_map[field_name]
        else:
            return ""

    def get_doc_words_by_field(self, field_name):
        """
        the return result could be list of str or a text
        :param field_name:
        :return:
        """
        text = self.get_doc_text_by_field(field_name)
        return text.split()

    def get_all_field_doc_map(self):
        return self.field_to_field_doc_map

    def get_name(self):
        return self.name

    def get_document_id(self):
        return self.id

    def get_document_text(self):
        """
        get all the text from this MultiFieldDocument by conbining text from all field.
        :return: return a str
        """
        docs = []
        for field_name in self.get_field_set():
            doc = self.get_doc_text_by_field(field_name=field_name)
            docs.append(doc)
        return "\n".join(docs)

    def get_document_text_words(self):
        """
        get all the text from this MultiFieldDocument by conbining text from all field.
        :return: return a iteration of str. each one is a word in doc field.
        """

        docs = []
        for field_name in self.get_field_set():
            doc = self.get_doc_words_by_field(field_name=field_name)
            docs.extend(doc)
        return docs

    def pretty_print(self):
        print("doc id=%r" % self.id)
        field_doc = self.get_all_field_doc_map()
        for field, doc in field_doc.items():
            print("field=%r doc=%r" % (field, doc))
        print("-" * 20)

    def __repr__(self):
        return "<MultiFieldDocument= id=%d name=%r doc=%r>" % (
            self.get_document_id(), self.get_name(), self.field_to_field_doc_map)

    def sub_doc(self, kept_fields):
        field_doc = {k: v for k, v in self.field_to_field_doc_map.items() if k in kept_fields}
        return MultiFieldDocument(id=self.id, name=self.name, field_to_field_doc_map=field_doc)


class MultiFieldDocumentCollection(SaveLoad):
    """
    This class is a wrapper for multi field document collection. It contain many MultiFieldDocument instance. Each one stand for a document.
    """

    def __init__(self, documents=None):
        self.documents = []
        self.field_set = set([])
        if documents:
            for document in documents:
                self.add_document(document)

        self.doc_id_2_documents_map = {}
        self.doc_id_2_doc_index_map = {}

    def get_num(self):
        return len(self.documents)

    def add_document(self, document: MultiFieldDocument):
        """
        :param document:
        :return: False, the doc with the id already exist. True, add new doc success
        """
        doc_id = document.id
        if doc_id in self.doc_id_2_documents_map.keys():
            return False
        self.documents.append(document)
        self.field_set.update(document.get_field_set())
        self.doc_id_2_documents_map[doc_id] = document
        self.doc_id_2_doc_index_map[doc_id] = len(self.documents) - 1
        return True

    def add_document_from_field_values(self,
                                       id, name, **field_to_field_doc_map):

        doc = MultiFieldDocument(id=id, name=name, **field_to_field_doc_map)
        return self.add_document(document=doc)

    def get_by_id(self, id):
        return self.doc_id_2_documents_map.get(id, None)

    def get_by_index(self, index):
        index = int(index)
        if index < 0 or index >= len(self.documents):
            return None
        return self.documents[index]

    def __str__(self):
        return "Documents(Num=%d)" % (self.get_num())

    def clear(self):
        self.documents = []
        self.field_set = set([])

    def get_field_set(self):
        return self.field_set

    def get_document_list(self):
        return self.documents

    def add_field_to_doc(self, doc_id, field_name, value):
        doc = self.get_by_id(id=doc_id)
        if doc is None:
            return
        doc.add_field(field_name, value)

    def doc_index_to_doc_id(self, index):
        return self.get_by_index(index).get_document_id()

    def doc_id_to_doc_index(self, doc_id):
        return self.doc_id_2_doc_index_map.get(doc_id, None)

    def get_doc_id_2_doc_index_map(self):
        return self.doc_id_2_doc_index_map

    def exist(self, id):
        """
        check if the doc with the specify id exist
        :param id: the doc id
        :return: True, exist,False, not exist
        """
        doc = self.get_by_id(id)
        if doc == None:
            return False
        return True

    def size(self):
        """
        get the doc num in this collection
        :return: the doc size
        """
        return self.get_num()

    def pretty_print_by_id(self, id):
        if not self.exist(id):
            print("Not exist doc for id=%r" % id)
        raw_doc = self.get_by_id(id)
        raw_doc.pretty_print()

    def random_docs(self, random_num):
        num = self.get_num()
        random_index_list = list(range(0, num))
        random.shuffle(random_index_list)
        random_index_list = random_index_list[:random_num]

        docs = []
        for index in random_index_list:
            doc = self.get_by_index(index)
            docs.append(doc)
        return docs

    def random_doc(self, ):
        num = self.get_num()
        random_index = random.randint(0, num)
        doc = self.get_by_index(random_index)
        return doc

    def doc_id_set_2_doc_index_set(self, doc_id_set):
        doc_index_set = set([])
        for doc_id in doc_id_set:
            doc_index = self.doc_id_to_doc_index(doc_id)
            if doc_index is not None:
                doc_index_set.add(doc_index)
        return doc_index_set

    def doc_index_set_2_doc_id_set(self, doc_index_set):
        doc_id_set = set([])
        for doc_index in doc_index_set:
            doc_id = self.doc_index_to_doc_id(doc_index)
            if doc_id is not None:
                doc_id_set.add(doc_id)
        return doc_id_set

    def sub_document_collection(self, doc_id_set):
        collection = MultiFieldDocumentCollection()
        for doc_id in doc_id_set:
            doc = self.get_by_id(doc_id)
            if doc != None:
                collection.add_document(doc)
        return collection
