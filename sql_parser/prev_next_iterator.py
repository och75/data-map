#!/usr/bin/python 
# -*- coding: utf-8 -*-

class PrevNextIterator(object):
    def __init__(self, collection):
        self.collection = collection
        self.index = -1

    def next(self):
        try:
            self.index += 1
            result_value = self.collection[self.index]
            result_index=self.index
        except IndexError:
            raise StopIteration
        return result_index, result_value

    def actual(self):
        result_value = self.collection[self.index]
        result_index = self.index
        return result_index, result_value

    def prev(self):
        self.index -= 1

        if self.index < 0:
            raise StopIteration

        result_value = self.collection[self.index]
        result_index = self.index

        return result_index, result_value

    def __iter__(self):
        return self