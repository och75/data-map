#!/usr/bin/python 
# -*- coding: utf-8 -*-
class SqlAbstractObject():

    base_string_literals=None

    BASE_DELIMITERS=None
    BASE_END_ENUM = None

    #nombre de parentheses ouvertes pendant l'iteration de ce Field dnas le cas ou le field est une composition de fonction :
    #if(coalesce(monChamp, 'rien'), 'rien', select max(mon champ) fron another table T  )
    #permet d'overrider les Field_delimiters
    nb_open_parenthesis_while_iter=None

    def __init__(self, BASE_DELIMITERS, BASE_END_ENUM):
        self.base_string_literals = []
        self.nb_open_parenthesis_while_iter=0
        self.BASE_DELIMITERS=BASE_DELIMITERS
        self.BASE_END_ENUM=BASE_END_ENUM


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
        stop_iterate_object = False if word not in one_object.BASE_END_ENUM else True

        one_object.nb_open_parenthesis_while_iter += 1 if word == '(' else 0
        one_object.nb_open_parenthesis_while_iter -= 1 if word == ')' else 0

        if (one_object.nb_open_parenthesis_while_iter > 0 or (word not in one_object.BASE_DELIMITERS and not stop_iterate_object)):

            one_object.field_string_literals.append(word)
            stop_iterate_object, one_object = SqlAbstractObject.iterate_one_object_strings(type_obj, enum, one_object)

        return stop_iterate_object, one_object

class SqlTable(SqlAbstractObject):

    def __init__(self):
        TABLE_DELIMITERS = ['innerjoin', 'leftouterjoin', 'crossjoin', 'rightouterjoin', 'on']
        TABLE_END_ENUM = ['where', ';', 'groupby', 'orderby']
        super( TABLE_DELIMITERS, TABLE_END_ENUM)

    @staticmethod
    def iterate_one_table_strings(enum, one_table=None):
        return SqlAbstractObject.iterate_one_object_strings('table',enum)

class SqlField(SqlAbstractObject):

    def __init__(self):
        FIELD_DELIMITERS = [',']
        FIELD_END_ENUM = ['from']
        super( FIELD_DELIMITERS, FIELD_END_ENUM)

    @staticmethod
    def iterate_one_field_strings(enum, one_table=None):
        return SqlAbstractObject.iterate_one_object_strings('field',enum)