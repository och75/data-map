#!/usr/bin/python 
# -*- coding: utf-8 -*-

class SqlTable():

    table_string_literals=[]

    TABLE_DELIMITERS=['innerjoin', 'leftouterjoin', 'crossjoin', 'rightouterjoin', 'on']
    TABLE_END_ENUM = ['where', ';', 'groupby', 'orderby']

    def __init__(self):
        self.table_string_literals = []

    @staticmethod
    def iterate_one_table_strings(enum, one_table=None):
        index, word = enum.next()

        if one_table is None:
            one_table=SqlTable()

        # si on rencontre "on" il faut avancer le curseur jusqu'qu prochain mot cle TABLE_DELIMITERS
        #TODO a deplacer dans interate from clause
        if word=='on':
            index, word = enum.next()
            while word not in SqlTable.TABLE_DELIMITERS and word not in SqlTable.TABLE_END_ENUM:
                index, word = enum.next()

        stop_iterate_tables = False if word not in SqlTable.TABLE_END_ENUM else True

        if word not in SqlTable.TABLE_DELIMITERS and not stop_iterate_tables:
            one_table.table_string_literals.append(word)
            stop_iterate_tables, one_table = SqlTable.iterate_one_table_strings(enum, one_table)

        return stop_iterate_tables, one_table

