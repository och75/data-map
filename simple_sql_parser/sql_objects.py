#!/usr/bin/python 
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from simple_sql_parser.prev_next_iterator import PrevNextIterator

class SqlClauseFrom:

    sql_tables=None
    sql_table_string=None

    def __init__(self):
        self.sql_table_string = ''

    @staticmethod
    def iterate_from_clause(enum, stop=False, f_sql_tables=[]):
        stop, one_table = SqlTable.iterate_one_table_strings(enum)
        f_sql_tables.append(one_table)

        index, word=enum.actual()

        #on ne parse pas les clauses de jointures  (mais on pourrait creer un objet JoinClause )
        while ( word=='on') or (word not in SqlTable.TABLE_DELIMITERS and not stop):
            index, word = enum.next()
            stop = word in SqlTable.TABLE_END_ENUM and not stop

        if not stop:
            stop, f_sql_tables = SqlClauseFrom.iterate_from_clause(enum, f_sql_tables)

        return stop, f_sql_tables


class SqlClauseSelect:

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

class SqlStatement():

    select_clause=None
    from_clause=None
    where_clause=None
    group_clause=None
    order_clause=None
    enum=None

    def __init__(self,enum):
        self.enum=enum
        self.parse_sql_statement()


    def parse_sql_statement(self):
        index, word = self.enum.next()
        if word == 'select':
            self.select_clause = SqlClauseSelect()
            stop, self.select_clause.sql_fields = SqlClauseSelect.iterate_select_clause(self.enum)

        index, word = self.enum.actual()
        if word == 'from':
            self.from_clause = SqlClauseFrom()
            stop, self.from_clause.sql_tables = SqlClauseFrom.iterate_from_clause(self.enum)

class SqlAbstractObject(ABC):

    strings_list=None

    #nombre de parentheses ouvertes pendant l'iteration de ce Field dnas le cas ou le field est une composition de fonction :
    #if(coalesce(monChamp, 'rien'), 'rien', select max(mon champ) fron another table T  )
    #permet d'overrider les Field_delimiters
    nb_open_parenthesis_while_iter=None

    sql_alias_name=None
    has_sub_select=None
    has_introspected=False
    sub_select=None

    def __init__(self):
        self.strings_list = []
        self.nb_open_parenthesis_while_iter=0

    def introspect(self):
        """
        intrsopection pour decouvrir le nom/alias de l'objet, decouvrir si cest une sous requete SQL etc
        :return:
        """
        if not self.has_introspected:
            index_select = self.strings_list.index('select') if 'select' in self.strings_list else -1
            index_select_end=-1
            par =0

            if index_select>-1:
                #creer un iterateur ne contenant pas les parentheses ouvrantes fermantes du sous select
                #la chaine doit s'arreter a la bonne parenthèse fermante , on va rechercher la derniere parenthèse fermante
                #a partir du point de depart index_select
                for index, word in enumerate(self.strings_list[index_select:] ) :
                    par += 1 if word == '(' else -1 if word == ')' else 0
                    index_select_end = index_select+index \
                        if par == -1 and index_select_end == -1 else index_select_end

                sub_enum=PrevNextIterator(self.strings_list[index_select :index_select_end])
                self.sub_select=SqlStatement(sub_enum)
                self.has_sub_select = True

            self.sql_alias_name= self.strings_list[-1]
            self.has_introspected=True

    @abstractmethod
    def get_obj_delimiters(self):
        return None

    @abstractmethod
    def get_obj_end_enum(self):
        return None


    @staticmethod
    def factory_child(type_obj):
        obj=None
        if type_obj=='table':
            obj=SqlTable()
        if type_obj=='field':
            obj=SqlField()

        return obj

    @staticmethod
    def iterate_one_object_strings(type_obj, enum, one_object=None):

        one_object = SqlAbstractObject.factory_child(type_obj) \
            if one_object is None else one_object

        index, word, stop_iterate_object= -1,'', False
        try:
            index, word = enum.next()
            stop_iterate_object = False if word not in one_object.get_obj_end_enum() else True
        except StopIteration:
            stop_iterate_object=True

        #on itere tant qu'on ne tombe pqs sur un mot delimitant
        # et qu'il n'y a pas de parentheses ouvertes, signe qu'on serait dans un sous select
        # qui lui aussi pourrait contenir des mots delimitants
        one_object.nb_open_parenthesis_while_iter += 1 if word == '(' else 0
        one_object.nb_open_parenthesis_while_iter -= 1 if word == ')' else 0

        if (one_object.nb_open_parenthesis_while_iter > 0
                or (word not in one_object.get_obj_delimiters() and not stop_iterate_object)
        ):
            one_object.strings_list.append(word)
            stop_iterate_object, one_object = SqlAbstractObject.iterate_one_object_strings(type_obj, enum, one_object)
        else:
            # fin de l'iterqtion, on lance l'introspection
            one_object.introspect()

        return stop_iterate_object, one_object

class SqlTable(SqlAbstractObject):
    TABLE_DELIMITERS = ['innerjoin', 'leftouterjoin', 'crossjoin', 'rightouterjoin', 'on']
    TABLE_END_ENUM = ['where', ';', 'groupby', 'orderby']

    def __init__(self):
        super().__init__()

    def get_obj_delimiters(self):
        return self.TABLE_DELIMITERS

    def get_obj_end_enum(self):
        return self.TABLE_END_ENUM

    @staticmethod
    def iterate_one_table_strings(enum):
        return SqlAbstractObject.iterate_one_object_strings('table',enum)

class SqlField(SqlAbstractObject):

    FIELD_DELIMITERS = [',']
    FIELD_END_ENUM = ['from']

    def __init__(self):
        super().__init__()

    def get_obj_delimiters(self):
        return self.FIELD_DELIMITERS

    def get_obj_end_enum(self):
        return self.FIELD_END_ENUM

    @staticmethod
    def iterate_one_field_strings(enum):
        return SqlAbstractObject.iterate_one_object_strings('field',enum)