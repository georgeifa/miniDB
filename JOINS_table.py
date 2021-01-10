import table




    def _left_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right). Show all rows from left table and the matched ones from the right one
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error
        try:
            column_index_left = self.column_names.index(column_name_left)
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Columns dont exist in one or both tables.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = f'{self._name}_left_join_{table_right._name}'
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # count the number of operations (<,> etc)
        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table

        #create a row with NoneObjects or 0  (for str is None , for bool is false and for numbers is 0)
        row_null =[]

        for row_left in self.data:
            row_null.clear() # clear the null row for every new row of the left table
            hasMatch =  False # a bool that shows if the current row of the left table has found a matching row from the right table
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                no_of_ops+=1
                if get_op(operator, left_value, right_value): #EQ_OP
                    join_table._insert(row_left+row_right)
                    hasMatch = True #if you find a matching row make it true
            if not hasMatch: #if no matching row is found
                for column in table_right.column_types: # for every column in the right table
                    if column == type(1) or column == type(1.2): # check the type of the column
                        row_null.append(0) #if its a number add 0 to the null_row list
                    else:
                        row_null.append(None)# else add none
                join_table._insert(row_left + row_null) # add the new row to the join table


        print(f'## Select ops no. -> {no_of_ops}')
        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')

        return join_table

    def _right_join(self, table_right: Table, condition):
            '''
            Join table (left) with a supplied table (right). Show all rows from right table and the matched ones from the left one
            '''
            # get columns and operator
            column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
            # try to find both columns, if you fail raise error
            try:
                column_index_left = self.column_names.index(column_name_left)
                column_index_right = table_right.column_names.index(column_name_right)
            except:
                raise Exception(f'Columns dont exist in one or both tables.')

            # get the column names of both tables with the table name in front
            # ex. for left -> name becomes left_table_name_name etc
            left_names = [f'{self._name}_{colname}' for colname in self.column_names]
            right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

            # define the new tables name, its column names and types
            join_table_name = f'{self._name}_right_join_{table_right._name}'
            join_table_colnames = left_names+right_names
            join_table_coltypes = self.column_types+table_right.column_types
            join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

            # count the number of operations (<,> etc)
            no_of_ops = 0
            # this code is dumb on purpose... it needs to illustrate the underline technique
            # for each value in left column and right column, if condition, append the corresponding row to the new table

            #right join works with a similar way as the left join. Only difference is that it is inverted
            row_null =[]


            for row_right in table_right.data:
                row_null.clear()
                hasMatch =  False
                right_value = row_right[column_index_right]
                for row_left in self.data:
                    left_value = row_left[column_index_left]
                    no_of_ops+=1
                    if get_op(operator, left_value, right_value): #EQ_OP
                        join_table._insert(row_left+row_right)
                        hasMatch = True
                if not hasMatch:
                    for column in self.column_types:
                        if column == type(1) or column == type(1.2):
                            row_null.append(0)
                        else:
                            row_null.append(None)
                    join_table._insert(row_null + row_right)


            print(f'## Select ops no. -> {no_of_ops}')
            print(f'# Left table size -> {len(self.data)}')
            print(f'# Right table size -> {len(table_right.data)}')

            return join_table

    def _outer_join(self, table_right: Table, condition):
            '''
            Join table (left) with a supplied table (right).
            Show all rows from both tables and match the rows where the condition is met
            '''
            # get columns and operator
            column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
            # try to find both columns, if you fail raise error
            try:
                column_index_left = self.column_names.index(column_name_left)
                column_index_right = table_right.column_names.index(column_name_right)
            except:
                raise Exception(f'Columns dont exist in one or both tables.')

            # get the column names of both tables with the table name in front
            # ex. for left -> name becomes left_table_name_name etc
            left_names = [f'{self._name}_{colname}' for colname in self.column_names]
            right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

            # define the new tables name, its column names and types
            join_table_name = f'{self._name}_outer_join_{table_right._name}'
            join_table_colnames = left_names+right_names
            join_table_coltypes = self.column_types+table_right.column_types
            join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

            # count the number of operations (<,> etc)
            no_of_ops = 0
            # this code is dumb on purpose... it needs to illustrate the underline technique
            # for each value in left column and right column, if condition, append the corresponding row to the new table
            row_null =[]

            #outer join works with a combination of the left and right join
            #ONLY DIFFERENCE when doing the right_join part we dont insert if there is a match. Only when there is not

            for row_left in self.data:
                row_null.clear()
                hasMatch =  False
                left_value = row_left[column_index_left]
                for row_right in table_right.data:
                    right_value = row_right[column_index_right]
                    no_of_ops+=1
                    if get_op(operator, left_value, right_value): #EQ_OP
                        join_table._insert(row_left+row_right)
                        hasMatch = True
                if not hasMatch:
                    for column in table_right.column_types:
                        if column == type(1) or column == type(1.2):
                            row_null.append(0)
                        else:
                            row_null.append(None)
                    join_table._insert(row_left + row_null)

            for row_right in table_right.data:
                row_null.clear()
                hasMatch =  False
                right_value = row_right[column_index_right]
                for row_left in self.data:
                    left_value = row_left[column_index_left]
                    no_of_ops+=1
                    if get_op(operator, left_value, right_value): #EQ_OP
                        hasMatch = True
                if not hasMatch:
                    for column in self.column_types:
                        if column == type(1) or column == type(1.2):
                            row_null.append(0)
                        else:
                            row_null.append(None)
                    join_table._insert(row_null + row_right)




            print(f'## Select ops no. -> {no_of_ops}')
            print(f'# Left table size -> {len(self.data)}')
            print(f'# Right table size -> {len(table_right.data)}')

            return join_table

    def _Index_Nested_Loop_join(self, table_right: Table, bt, condition ):
        '''
        Join table (left) with a supplied table (right) where condition is met.
        The search through the right table will be done by indexes
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error
        try:
            column_index_left = self.column_names.index(column_name_left)
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Columns dont exist in one or both tables.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = f'{self._name}_Index_Nested_Loop_join_{table_right._name}'
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)


        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table

        # we check that column that the condition is on is the PK as indexes support only this column
        if column_index_right != table_right.pk_idx:
            raise Exception(f'Column is not PK. Indexes suport only PK columns. Aborting...')

        # for every row in the left table
        for row_left in self.data:
            left_value = row_left[column_index_left] # take the value on the condition

            row_right = [] # create an empty list for the right row (mostly so that the code will look nicer)
            row_right_index = bt.find(operator, left_value) # get the index of the row that matches with the condition

            if len(row_right_index) > 0: # if there is a row found
                row_right = table_right.data[row_right_index[0]] # get the row from the right table with the row index from above
                join_table._insert(row_left+row_right)

        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')

        return join_table

    def _sort_merge_join(self, table_right: Table, condition , asc = False):
        '''
        Join table (left) with a supplied table (right). Show all rows from left table and the matched ones from the right one
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error
        try:
            column_index_left = self.column_names.index(column_name_left)
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Columns dont exist in one or both tables.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = f'{self._name}_sort_merge_join_{table_right._name}'
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)


        # count the number of operations (<,> etc)
        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table

        # we check that column that the condition is on is the PK as indexes support only this column
        if column_index_right != table_right.pk_idx:
            raise Exception(f'Column is not PK. Indexes suport only PK columns. Aborting...')

        # we are sorting both tables
        self._sort(column_name_left, asc=asc)
        table_right._sort(column_name_right, asc=asc)


        for row_left in self.data:
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                no_of_ops+=1
                if get_op(operator, left_value, right_value): #EQ_OP
                    join_table._insert(row_left+row_right) #until here is the same as inner_join
                else: #if it doesnt match compare the values
                    if asc: #if the sorting is ascending
                        if operator == '==': #if we want equal values we stop searching when the right value is greater than the left
                            if left_value < right_value:
                                break
                        elif operator == "<":#if we want lesser left value we stop searching when the left value is greater or equal than the right
                            if left_value >= right_value:
                                break
                        elif operator == "<=":#if we want lesser or equal left value we stop searching when the left value is greater than the right
                            if left_value > right_value:
                                break
                        elif operator == ">=":#if we want greater or equal left value we stop searching when the right value is greater than the left
                            if left_value < right_value:
                                break
                        elif operator == ">": #if we want greater left value we stop searching when the right value is greater or eqaul than the left
                            if left_value <= right_value:
                                break
                    else: #if the sorting is descending. Here the conditions are the opossites of the ascending ones
                        if operator == '==':
                            if left_value >= right_value:
                                break
                        elif operator == "<":
                            if left_value < right_value:
                                break
                        elif operator == "<=":
                            if left_value <= right_value:
                                break
                        elif operator == ">=":
                            if left_value >= right_value:
                                break
                        elif operator == ">":
                            if left_value > right_value:
                                break


        #checked that it does less ops no.
        print(f'## Select ops no. -> {no_of_ops}')
        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')

        return join_table
