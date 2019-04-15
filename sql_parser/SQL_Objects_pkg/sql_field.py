#!/usr/bin/python 
# -*- coding: utf-8 -*-

class SqlField():

    field_string_literals=[]

    FIELD_DELIMITERS=[',']
    FIELD_END_ENUM = ['from']

    #nombre de parentheses ouvertes pendant l'iteration de ce Field dnas le cas ou le field est une composition de fonction :
    #if(coalesce(monChamp, 'rien'), 'rien', select max(mon champ) fron another table T  )
    #permet d'overrider les Field_delimiters
    nb_open_parenthesis_while_iter=None

    def __init__(self):
        self.field_string_literals = []
        self.nb_open_parenthesis_while_iter=0

    @staticmethod
    def iterate_one_field_strings(enum, one_field=None):
        index, word = enum.next()
        stop_iterate_fields = False if word not in SqlField.FIELD_END_ENUM else True

        if one_field is None:
            one_field = SqlField()

        one_field.nb_open_parenthesis_while_iter += 1 if word == '(' else 0
        one_field.nb_open_parenthesis_while_iter -= 1 if word == ')' else 0

        if (one_field.nb_open_parenthesis_while_iter > 0 or (word not in SqlField.FIELD_DELIMITERS and not stop_iterate_fields)):

            one_field.field_string_literals.append(word)
            stop_iterate_fields, one_field = SqlField.iterate_one_field_strings(enum, one_field)

        return stop_iterate_fields, one_field


