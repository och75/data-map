#!/usr/bin/python 
# -*- coding: utf-8 -*-

class SqlField():

    field_string_literals=[]

    FIELD_DELIMITERS=[',']
    FIELD_END_ENUM = ['from']

    def __init__(self):
        self.field_string_literals = []

    @staticmethod
    def iterate_one_field_strings(enum, one_field=None):
        index, word = enum.next()
        stop_iterate_fields = False if word not in SqlField.FIELD_END_ENUM else True

        if one_field is None:
            one_field = SqlField()

        if word not in SqlField.FIELD_DELIMITERS and not stop_iterate_fields:
            one_field.field_string_literals.append(word)
            stop_iterate_fields, one_field = SqlField.iterate_one_field_strings(enum, one_field)

        return stop_iterate_fields, one_field


