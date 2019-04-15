#!/usr/bin/python 
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod

class SqlAbstractObject(ABC):

    base_string_literals=None

    #nombre de parentheses ouvertes pendant l'iteration de ce Field dnas le cas ou le field est une composition de fonction :
    #if(coalesce(monChamp, 'rien'), 'rien', select max(mon champ) fron another table T  )
    #permet d'overrider les Field_delimiters
    nb_open_parenthesis_while_iter=None

    def __init__(self):
        self.base_string_literals = []
        self.nb_open_parenthesis_while_iter=0

    @abstractmethod
    def get_obj_delimiters(self):
        return None

    @abstractmethod
    def get_obj_end_enum(self):
        return None


    @staticmethod
    def factory_children(type_obj):
        obj=None
        if type_obj=='table':
            obj=SqlTable()
        if type_obj=='field':
            obj=SqlField()

        return obj

    @staticmethod
    def iterate_one_object_strings(type_obj, enum, one_object=None):
        if one_object is None:
            one_object = SqlAbstractObject.factory_children(type_obj)

        index, word = enum.next()
        stop_iterate_object = False if word not in one_object.get_obj_end_enum() else True

        one_object.nb_open_parenthesis_while_iter += 1 if word == '(' else 0
        one_object.nb_open_parenthesis_while_iter -= 1 if word == ')' else 0

        if (one_object.nb_open_parenthesis_while_iter > 0 or (word not in one_object.get_obj_delimiters() and not stop_iterate_object)):

            one_object.base_string_literals.append(word)
            stop_iterate_object, one_object = SqlAbstractObject.iterate_one_object_strings(type_obj, enum, one_object)

        return stop_iterate_object, one_object

class SqlTable(SqlAbstractObject):
    TABLE_DELIMITERS = ['innerjoin', 'leftouterjoin', 'crossjoin', 'rightouterjoin', 'on']
    TABLE_END_ENUM = ['where', ';', 'groupby', 'orderby']

    def __init__(self):
        super().__init__()

    def get_obj_delimiters(self):
        return self.TABLE_DELIMITERS

    def get_obj_end_enum(self):
        return self.TABLE_END_ENUM

    @staticmethod
    def iterate_one_table_strings(enum, one_table=None):
        return SqlAbstractObject.iterate_one_object_strings('table',enum)

class SqlField(SqlAbstractObject):

    FIELD_DELIMITERS = [',']
    FIELD_END_ENUM = ['from']

    def __init__(self):
        super().__init__()

    def get_obj_delimiters(self):
        return self.FIELD_DELIMITERS

    def get_obj_end_enum(self):
        return self.FIELD_END_ENUM

    @staticmethod
    def iterate_one_field_strings(enum, one_table=None):
        return SqlAbstractObject.iterate_one_object_strings('field',enum)