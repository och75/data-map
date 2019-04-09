#!/usr/bin/python 
# -*- coding: utf-8 -*-
from SqlClauseSelect import SqlClauseSelect
from SqlClauseFrom import SqlClauseFrom

class SqlStatement():

    select_clause=None
    from_clause=None
    where_clause=None
    group_clause=None
    order_clause=None
    enum=None

    def __init__(self,enum):
        self.enum=enum


    def parse_sql_statement(self):
        index, word = self.enum.next()
        if word == 'select':
            self.select_clause = SqlClauseSelect()
            stop, self.select_clause.sql_fields = SqlClauseSelect.iterate_select_clause(self.enum)

        index, word = self.enum.actual()
        if word == 'from':
            self.from_clause = SqlClauseFrom()
            stop, self.from_clause.sql_tables = SqlClauseFrom.iterate_from_clause(self.enum)
