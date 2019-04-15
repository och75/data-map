#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, re

root = "/Users/o.charles/PycharmProjects/dw-scripts/airflow/dags"
list_py_files = {}


#TODO gestion des subDag ?
pattern_dag=r"dag_id='(\w+)',schedule_interval"
pattern_sql=r"((\w+/){0,}(\w+)\.sql)"

for path, subdirs, files in os.walk(root):
    for file_name in files:
        if (file_name[-2:].lower() == 'py') and not (file_name[0].lower() == '.'):

            full_path=os.path.join(path, file_name)
            list_py_files[full_path]={
                                        'dag_id':'',
                                        'sql_files':[]
                                    }

            for line in open(path+'/'+file_name):
                #search for dag_name
                list_dag=re.findall(pattern_dag,line.replace(' ',''))
                if len(list_dag)>0:
                    list_py_files[full_path]['dag_id'] = list_dag[0]

                #search for sql files
                list_sql=re.findall(pattern_sql,line)
                if len(list_sql)>0:
                    list_sql_files=[item for item in list_sql[0] if item[-4:]=='.sql']
                    list_py_files[full_path]['sql_files'].append(list_sql_files[0])
                    print('rien')

print ('end')

#https://stackoverflow.com/questions/454456/how-do-i-re-search-or-re-match-on-a-whole-file-without-reading-it-all-into-memor
#recherche SQL script
#pattern=r"(((\w+)/)?(\w+).sql)"
#re.findall(pattern,line)
#[('warehouse/dim_member_notification_info.sql', 'warehouse/', 'warehouse', 'dim_member_notification_info')]

#https://pypi.org/project/sqlparse/
#https://stackoverflow.com/questions/1394998/parsing-sql-with-python
#https://stackoverflow.com/questions/39289898/parsing-sql-queries-in-python
#https://stackoverflow.com/questions/29646908/parsing-sql-with-python-to-find-specific-statements
