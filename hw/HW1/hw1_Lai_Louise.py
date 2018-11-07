"""
Created on October, 2018

@author: Diane Woodbridge
"""

import sys
import os


def function_with_many_many_argunemts(variable_1, variable_2,
                                      variable_3, variable_4):
    print("wow")


def another_function():
    print("wow")


def insert(data):
    preprocessed_data = [1, 2]


def update(data=None):
    return True


def drop(db, table):
    """
    Designed specifically for MonogoDB.
    Todo: extend it for other DBs.
    """
    find(db).drop(table)
    # An error will occur, if db or table doesn't exist.


def delete_from_table(data):  # Fix this: Readability?
    processed_data = preprocess(data)
    return False
