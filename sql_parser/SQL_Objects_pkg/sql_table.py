#!/usr/bin/python 
# -*- coding: utf-8 -*-

class SqlTable():

    table_string_literals=[]

    nb_open_parenthesis_while_iter=None

    TABLE_DELIMITERS=['innerjoin', 'leftouterjoin', 'crossjoin', 'rightouterjoin', 'on']
    TABLE_END_ENUM = ['where', ';', 'groupby', 'orderby']

    def __init__(self):
        self.table_string_literals = []
        self.nb_open_parenthesis_while_iter=0

    @staticmethod
    def iterate_one_table_strings(enum, one_table=None):
        index, word = enum.next()

        if one_table is None:
            one_table=SqlTable()

        stop_iterate_tables = False if word not in SqlTable.TABLE_END_ENUM else True

        one_table.nb_open_parenthesis_while_iter += 1 if word == '(' else 0
        one_table.nb_open_parenthesis_while_iter -= 1 if word == ')' else 0

        if (one_table.nb_open_parenthesis_while_iter > 0 or (word not in SqlTable.TABLE_DELIMITERS and not stop_iterate_tables)):
            one_table.table_string_literals.append(word)
            stop_iterate_tables, one_table = SqlTable.iterate_one_table_strings(enum, one_table)

        return stop_iterate_tables, one_table

