from simple_sql_parser.sql_parser import SQLParser

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
                "  select if(coalesce(monChamp, 'rien'), 'rien', ( select max(mon champ) from another_table T ) ) " \
                "  from table3" \
                "  where table3.truc='uneValeur' " \
                ") t3 " \
                "    on t2.k=t3.k " \
                "left outer join table4 on t3.k=t4.k " \
                "where " \
                "  (toto>'2019' Or momo='toutoitit' );"

#cas1=SQLParser(str_sql_statement)


str_sql_statement = "select " \
                "  monchamp as label1 " \
                "from " \
                "( " \
                "  select if(coalesce(monChamp, 'rien'), 'rien', ( select max(mon champ) from another_table T ) ) " \
                "  from table3" \
                "  where table3.truc='uneValeur' " \
                ") t3 "

cas2=SQLParser(str_sql_statement)

print('end')