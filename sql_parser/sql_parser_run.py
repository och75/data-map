#!/usr/bin/python 
# -*- coding: utf-8 -*-

str_sql_statement = "select " \
                "  monchamp as label1," \
                "  momautrechamp as label2, " \
                "  if(t1.toto is null, 2,1) as rank, " \
                "  field4 " \
                "from " \
                "  table1 as t1 " \
                "inner join table2 as t2 " \
                "    on t1.k=t2.k " \
                "inner join ( " \
                "  select if(coalesce(monChamp, 'rien'), 'rien', select max(mon champ) from another table T  ) " \
                "  from table3" \
                "  where table3.truc='uneValeur' " \
                ") t3 " \
                "    on t2.k=t3.k " \
                "left outer join table4 on t3.k=t4.k " \
                "where " \
                "  (toto>'2019' Or momo='toutoitit' );"

# pattern_select=r"select(.*)from"
# pattern_from=r"from(.*)[(where)|(group by)|(order by)|;]"
# pattern_join=r"(.*)[(inner join)|(left outer join)|(cross join)|(on)]"
# pattern_where="where(.*)[(and)|(group by)|(order by)|;]"
# pattern_groupby="group by(.*)[(order by)|;]"
# pattern_orderby="order by(.*);"

keywords = [',', '(', ')']

sql_reformated = str_sql_statement

# formatage de la chaine , on met en place les espaces autour des virgules, parentheses
for kw in keywords:
    sql_reformated = sql_reformated.replace(kw, ' {} '.format(kw))

# les mots cles composes sont regroupes en un seul mot sans espace
for mot_compose in ['inner join', 'left outer join', 'right outer join', 'group by', 'order by']:
    sql_reformated = sql_reformated.replace(mot_compose, mot_compose.replace(' ', ''))

# on vire les espaces en doublons
while sql_reformated.find('  ') > 0:
    sql_reformated = sql_reformated.replace('  ', ' ')

# normalement, on a ici une liste separees par des espaces ou chaque "mot" est signifiant
words = sql_reformated.split()

from sql_parser.prev_next_iterator import PrevNextIterator
from sql_parser.SQL_Objects_pkg.sql_statement import SqlStatement

enum = PrevNextIterator(words)

sql_statement = SqlStatement(enum)

print('end')
