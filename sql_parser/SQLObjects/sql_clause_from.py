#!/usr/bin/python 
# -*- coding: utf-8 -*-
from sql_table import SqlTable


class SqlClauseFrom():

    sql_tables=None
    sql_table_string=None

    def __init__(self):
        self.sql_table_string = ''

    @staticmethod
    def iterate_from_clause(enum, stop=False, sql_tables=[]):
        stop, one_table = SqlTable.iterate_one_table_strings(enum)
        sql_tables.append(one_table)

        index, word=enum.actual()

        #on ne parse pas les clauses de jointures  (mais on pourrait creer un objet JoinClause )
        while ( word=='on') or (word not in SqlTable.TABLE_DELIMITERS and not stop):
            index, word = enum.next()
            stop = word in SqlTable.TABLE_END_ENUM and not stop

        if not stop:
            stop, sql_tables = SqlClauseFrom.iterate_from_clause(enum,sql_tables)

        return stop, sql_tables
