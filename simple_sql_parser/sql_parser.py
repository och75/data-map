#!/usr/bin/python 
# -*- coding: utf-8 -*-
from simple_sql_parser.prev_next_iterator import PrevNextIterator
from simple_sql_parser.sql_objects import SqlStatement

class SQLParser:
    SEP = [',', '(', ')']
    str_sql_statement=''
    enum=None
    sql_statement=None

    def __init__(self,str_sql_statement):
        self.str_sql_statement=str_sql_statement
        self.enum = PrevNextIterator(
            self.format_and_prepare_sql()
        )
        self.sql_statement = SqlStatement(self.enum)

    def  format_and_prepare_sql(self):
        sql_reformated = self.str_sql_statement
        # formatage de la chaine , on met en place les espaces autour des virgules, parentheses
        for kw in self.SEP:
            sql_reformated = sql_reformated.replace(kw, ' {} '.format(kw))

        # les mots cles composes sont regroupes en un seul mot sans espace
        for mot_compose in ['inner join', 'left outer join', 'right outer join', 'group by', 'order by']:
            sql_reformated = sql_reformated.replace(mot_compose, mot_compose.replace(' ', ''))

        # on vire les espaces en doublons
        while sql_reformated.find('  ') > 0:
            sql_reformated = sql_reformated.replace('  ', ' ')

        # normalement, on a ici une liste separees par des espaces ou chaque "mot" est signifiant
        return sql_reformated.split()


# pattern_select=r"select(.*)from"
# pattern_from=r"from(.*)[(where)|(group by)|(order by)|;]"
# pattern_join=r"(.*)[(inner join)|(left outer join)|(cross join)|(on)]"
# pattern_where="where(.*)[(and)|(group by)|(order by)|;]"
# pattern_groupby="group by(.*)[(order by)|;]"
# pattern_orderby="order by(.*);"