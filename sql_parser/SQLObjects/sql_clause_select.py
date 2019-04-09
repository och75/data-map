#!/usr/bin/python 
# -*- coding: utf-8 -*-
from sql_field import SqlField


class SqlClauseSelect():

    sql_fields=None

    def __init__(self):
        self.sql_fields = []

    @staticmethod
    def iterate_select_clause(enum, stop=False, sql_fields=[]):
        stop, one_field = SqlField.iterate_one_field_strings(enum)
        sql_fields.append(one_field)

        if not stop:
            stop, sql_fields = SqlClauseSelect.iterate_select_clause(enum, sql_fields)

        return stop, sql_fields


