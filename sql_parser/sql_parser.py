#!/usr/bin/python 
# -*- coding: utf-8 -*-

sql_statement="select monchamp as label1,momautrechamp as label2 from table1 as t1 inner join table2 as t2 on t1.k=t2.k inner join table3 t3 on t3.k=t2.k  where (toto>'2019' Or momo='toutoitit' );"

#pattern_select=r"select(.*)from"
#pattern_from=r"from(.*)[(where)|(group by)|(order by)|;]"
#pattern_join=r"(.*)[(inner join)|(left outer join)|(cross join)|(on)]"
#pattern_where="where(.*)[(and)|(group by)|(order by)|;]"
#pattern_groupby="group by(.*)[(order by)|;]"
#pattern_orderby="order by(.*);"

keywords=[',','(',')']

sql_reformated=sql_statement

for kw in keywords:
    sql_reformated=sql_reformated.replace(kw, ' {} '.format(kw))

for mot_compose in ['inner join', 'left outer join', 'right outer join', 'group by', 'order by']:
    sql_reformated=sql_reformated.replace(mot_compose, mot_compose.replace(' ',''))

while sql_reformated.find('  ')>0:
    sql_reformated=sql_reformated.replace('  ',' ')

words=sql_reformated.split()




from SQLObjects.SqlStatement import SqlStatement
from PrevNextIterator import PrevNextIterator

enum=PrevNextIterator(words)

sql_statement=SqlStatement(enum)
sql_statement.parse_sql_statement()



print('end')








