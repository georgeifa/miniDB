from __future__ import annotations
import pickle
from table import Table
from time import sleep, localtime, strftime
import os
from btree import Btree
import shutil
from misc import split_condition
import database

    def left_join(self, left_table_name, right_table_name, condition, save_as=None, return_object=False):
        '''
        Join two tables that are part of the database where condition is met.
        left_table_name -> left table's name (needs to exist in database)
        right_table_name -> right table's name (needs to exist in database)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.
                    operatores supported -> (<,<=,==,>=,>)
        save_as -> The name that will be used to save the resulting table in the database. Def: None (no save)
        return_object -> If true, the result will be a table object (usefull for internal usage). Def: False (the result will be printed)
        '''
        self.load(self.savedir)
        if self.is_locked(left_table_name) or self.is_locked(right_table_name):
            print(f'Table/Tables are currently locked')
            return

        res = self.tables[left_table_name]._left_join(self.tables[right_table_name], condition)
        if save_as is not None:
            res._name = save_as
            self.table_from_object(res)
        else:
            if return_object:
                return res
            else:
                res.show()

    def right_join(self, left_table_name, right_table_name, condition, save_as=None, return_object=False):
        '''
        Join two tables that are part of the database where condition is met.
        left_table_name -> left table's name (needs to exist in database)
        right_table_name -> right table's name (needs to exist in database)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.
                    operatores supported -> (<,<=,==,>=,>)
        save_as -> The name that will be used to save the resulting table in the database. Def: None (no save)
        return_object -> If true, the result will be a table object (usefull for internal usage). Def: False (the result will be printed)
        '''
        self.load(self.savedir)
        if self.is_locked(left_table_name) or self.is_locked(right_table_name):
            print(f'Table/Tables are currently locked')
            return

        res = self.tables[left_table_name]._right_join(self.tables[right_table_name], condition)
        if save_as is not None:
            res._name = save_as
            self.table_from_object(res)
        else:
            if return_object:
                return res
            else:
                res.show()

    def outer_join(self, left_table_name, right_table_name, condition, save_as=None, return_object=False):
        '''
        Join two tables that are part of the database where condition is met.
        left_table_name -> left table's name (needs to exist in database)
        right_table_name -> right table's name (needs to exist in database)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.
                    operatores supported -> (<,<=,==,>=,>)
        save_as -> The name that will be used to save the resulting table in the database. Def: None (no save)
        return_object -> If true, the result will be a table object (usefull for internal usage). Def: False (the result will be printed)
        '''
        self.load(self.savedir)
        if self.is_locked(left_table_name) or self.is_locked(right_table_name):
            print(f'Table/Tables are currently locked')
            return

        res = self.tables[left_table_name]._outer_join(self.tables[right_table_name], condition)
        if save_as is not None:
            res._name = save_as
            self.table_from_object(res)
        else:
            if return_object:
                return res
            else:
                res.show()

    def index_nested_loop_join(self, left_table_name, right_table_name, condition, save_as=None, return_object=False):
        '''
        Join two tables that are part of the database where condition is met.
        left_table_name -> left table's name (needs to exist in database)
        right_table_name -> right table's name (needs to exist in database)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.
                    operatores supported -> (<,<=,==,>=,>)
        save_as -> The name that will be used to save the resulting table in the database. Def: None (no save)
        return_object -> If true, the result will be a table object (usefull for internal usage). Def: False (the result will be printed)
        '''
        self.load(self.savedir)
        if self.is_locked(left_table_name) or self.is_locked(right_table_name):
            print(f'Table/Tables are currently locked')
            return

            #check if the right table (the inner one) has an index on its PK
        if not self._has_index(right_table_name):
            print(f'The inner table (right one) does not have a index on its PK. An index is needed for this join.')
            return
        else:
            index_name = self.select('meta_indexes', '*', f'table_name=={right_table_name}', return_object=True).index_name[0]
            bt = self._load_idx(index_name)

        res = self.tables[left_table_name]._Index_Nested_Loop_join(self.tables[right_table_name], bt, condition)
        if save_as is not None:
            res._name = save_as
            self.table_from_object(res)
        else:
            if return_object:
                return res
            else:
                res.show()





    def sort_merge_join(self, left_table_name, right_table_name, condition, asc=False, save_as=None, return_object=False):
        '''
        Join two tables that are part of the database where condition is met.
        left_table_name -> left table's name (needs to exist in database)
        right_table_name -> right table's name (needs to exist in database)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.
                    operatores supported -> (<,<=,==,>=,>)
        save_as -> The name that will be used to save the resulting table in the database. Def: None (no save)
        return_object -> If true, the result will be a table object (usefull for internal usage). Def: False (the result will be printed)
        '''
        self.load(self.savedir)
        if self.is_locked(left_table_name) or self.is_locked(right_table_name):
            print(f'Table/Tables are currently locked')
            return

        res = self.tables[left_table_name]._sort_merge_join(self.tables[right_table_name], condition , asc = asc)
        if save_as is not None:
            res._name = save_as
            self.table_from_object(res)
        else:
            if return_object:
                return res
            else:
                res.show()
