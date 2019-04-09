#!/usr/bin/python 
# -*- coding: utf-8 -*-
from SqlTable import SqlTable


class SqlClauseFrom():

    sql_tables=None
    sql_table_string=None

    def __init__(self):
        self.sql_table_string = ''

    @staticmethod
    def iterate_from_clause(enum, stop=False, sql_tables=[]):
        stop, one_table = SqlTable.iterate_one_table_strings(enum)
        sql_tables.append(one_table)

        if not stop:
            stop, sql_tables = SqlClauseFrom.iterate_from_clause(enum,sql_tables)

        return stop, sql_tables
