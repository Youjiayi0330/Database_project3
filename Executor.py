import json
import os
import re
import csv
from BTrees.OOBTree import OOBTree
from FileConverter import FileConverter

from Parser import Parser


class Executor:
    def __init__(self, sql_command, database_name):
        self.sql = sql_command
        self.database_name = database_name
        self.directory_path = os.path.join(os.getcwd(), self.database_name)
        self.fileConverter = FileConverter()

    def _index_to_dict(self, index):
        return {str(key): value for key, value in index.items()}

    def _validate_foreign_key(self, foreign_keys):
        if foreign_keys:
            for fk_column, ref_table in foreign_keys.items():
                directory_path = os.path.join(os.getcwd(), self.database_name)
                json_file_path = os.path.join(directory_path, ref_table + '.json')
                if not os.path.exists(json_file_path):
                    print(f"Referenced table '{ref_table}' does not exist.")
                    return False
        return True

    def _validate_none_in_col_val(self, agg_col):
        if None in agg_col:
            return True
        return False

    def _filter_rows(self, condition, rows):
        filtered_rows = []
        parser = Parser(condition)
        column, op, value = parser.parse_condition()
        for row in rows:
            row_value = row[column]  # 安全获取列值
            if row_value is None:
                continue  # 如果行中没有该列，跳过
            # 根据操作符和值的类型执行比较
            if op == '=' and row_value == value:
                filtered_rows.append(row)
            elif op == '!=' and row_value != value:
                filtered_rows.append(row)
            elif op == '>' and row_value > value:
                filtered_rows.append(row)
            elif op == '<' and row_value < value:
                filtered_rows.append(row)
            elif op == 'LIKE' and isinstance(value, str) and value in str(row_value):
                filtered_rows.append(row)
        return filtered_rows

    def _filter_rows_by_index(self, condition, data):
        parser = Parser(condition)
        column, op, value = parser.parse_condition()
        # Check whether column is index
        index_list = data['indexes']
        if column in index_list:
            # 判断列类型，假设有类型信息可以访问，这里简化处理为直接指定
            coltype = data['columns'][column]
            index_tree = index_list[column]
            # 根据操作符和列类型获取相应的记录
            if op == '=':
                filtered_recordslist = index_tree.values(min=value, max=value)
            elif op == '>=':
                filtered_recordslist = index_tree.values(min=value)
            elif op == '<=':
                filtered_recordslist = index_tree.values(max=value)
            elif op == '!=':
                filtered_recordslist = [val for key, val in index_tree.items() if key != value]
            elif op == '>':
                if coltype.upper() == 'INT':
                    filtered_recordslist = index_tree.values(min=value + 1)
                else:
                    filtered_recordslist = index_tree.values(min=str(value) + ' ')
            elif op == '<':
                if coltype.upper() == 'INT':
                    filtered_recordslist = index_tree.values(max=value - 1)
                else:
                    maxvalue = value[:-1] + chr(ord(value[-1]) - 1)
                    filtered_recordslist = index_tree.values(max=maxvalue)

            # 展开记录列表
            filtered_pk = [item for sublist in filtered_recordslist for item in sublist]
            # 从数据中获取匹配的记录
            filtered_records = [data['data'][pk] for pk in filtered_pk]
        else:
            index_to_dict_rows = []
            # 使用items()方法遍历BTree中的所有项
            for key, value in data['data'].items():
                # 每个键值对转换为字典，并添加到列表中
                index_to_dict_rows.append(value)
            filtered_records = self._filter_rows(condition, index_to_dict_rows)
        return filtered_records

    def _set_selectivity(self, column_name, joined_rows):
        # parser = Parser(condition)
        # column_name, op, value = parser.parse_condition()
        column_value_list = []
        for joined_row in joined_rows:
            column_value = joined_row[column_name]
            column_value_list.append(column_value)
        unique_values = set(column_value_list)
        return len(unique_values) / len(column_value_list)

    def _comparison(self, sign, num, target_num):
        boo = False
        if sign == '=':
            return num == target_num
        elif sign == '!=':
            return num != target_num
        elif sign == '>=':
            return num >= target_num
        elif sign == '<=':
            return num <= target_num
        elif sign == '>':
            return num > target_num
        elif sign == '<':
            return num < target_num
        else:
            raise ValueError(f"Unsupported comparis")

    def _aggregate_col(self, agg_function, col): #col is a list where functions perform on
        agg_function = agg_function.lower()
        if agg_function == 'sum':
            return sum(col)
        elif agg_function == 'count':
            return len(col)
        elif agg_function == 'min':
            return min(col)
        elif agg_function == 'max':
            return max(col)
        elif agg_function == 'avg':
            return sum(col) / len(col) if col else 0
        else:
            raise ValueError(f"Unsupported aggregation function: {agg_function}")

    def _aggregate_col_select(self, agg_function, col):  # col is a list where functions perform on
        agg_function = agg_function.lower()
        if agg_function.upper() == 'SUM':
            return {sum(col): -1}
        elif agg_function.upper() == 'COUNT':
            return {len(col): -1}
        elif agg_function.upper() == 'MIN':
            min_val = min(col)
            min_id = [index for index, value in enumerate(col) if value == min_val]
            return {min_val: min_id}  # min_id = []
        elif agg_function.upper() == 'MAX':
            max_val = max(col)
            max_id = [index for index, value in enumerate(col) if value == max_val]
            return {max_val: max_id}  # max_id = []
        elif agg_function.upper() == 'AVG':
            avg = sum(col) / len(col) if col else 0
            return {avg: -1}
        else:
            raise ValueError(f"Unsupported aggregation function: {agg_function}")

    def _group_by_having(self, cur_table, target_col, having_clause):  # target_col: []
        group = set()
        for row in cur_table:
            values = []
            for col in target_col:
                values.append(row[col])
            group.add(tuple(values))
        grouped_table = []
        group_dict = {}
        for i, element in enumerate(group):
            group_dict[element] = i
            grouped_table.append([])
        for row in cur_table:
            values = []
            for col in target_col:
                values.append(row[col])
            grouped_table[group_dict[tuple(values)]].append(row)

        # Execute having
        rows_filtered_by_having = []
        if having_clause is None:
            rows_filtered_by_having = grouped_table
        else:
            parser = Parser(having_clause)
            conditions, logic_clause = parser.parse_clause()  # conditions: [condition1, condition2]
            # case when contains single condition
              # list of list
            if len(conditions) == 1:
                agg_function, distinct_clause, agg_column_name, operator, target_value = parser.parser_aggregate(
                    conditions[0])
                for sub_table in grouped_table:
                    agg_col_vals = [row[agg_column_name] for row in sub_table]
                    aggregate_value = self._aggregate_col(agg_function, agg_col_vals)

                    boo = self._comparison(operator, aggregate_value, target_value)

                    if boo:
                        rows_filtered_by_having.append(sub_table)

            else:
                agg_function_left, distinct_clause_left, agg_column_name_left, operator_left, target_value_left = parser.parser_aggregate(
                    conditions[0])
                agg_function_right, distinct_clause_right, agg_column_name_right, operator_right, target_value_right = parser.parser_aggregate(
                    conditions[1])
                # AND
                if logic_clause == "AND":
                    for sub_table in grouped_table:
                        agg_col_vals_1 = [row[agg_column_name_left] for row in sub_table]
                        if self._validate_none_in_col_val(agg_col_vals_1):
                            return print(f"NoneType exist! Please process None firstly!")
                        aggregate_value_1 = self._aggregate_col(agg_function_left, agg_col_vals_1)

                        agg_col_vals_2 = [row[agg_column_name_right] for row in sub_table]
                        if self._validate_none_in_col_val(agg_col_vals_2):
                            return print(f"NoneType exist! Please process None firstly!")
                        aggregate_value_2 = self._aggregate_col(agg_function_right, agg_col_vals_2)

                        boo1 = self._comparison(operator_left, aggregate_value_1, target_value_left)
                        boo2 = self._comparison(operator_right, aggregate_value_2, target_value_right)

                        if boo1 and boo2:
                            rows_filtered_by_having.append(sub_table)
                # OR
                else:
                    temp_set = set()
                    for sub_table in grouped_table:
                        agg_col_vals_1 = [row[agg_column_name_left] for row in sub_table]
                        if self._validate_none_in_col_val(agg_col_vals_1):
                            return print(f"NoneType exist! Please process None firstly!")
                        aggregate_value_1 = self._aggregate_col(agg_function_left, agg_col_vals_1)

                        agg_col_vals_2 = [row[agg_column_name_right] for row in sub_table]
                        if self._validate_none_in_col_val(agg_col_vals_2):
                            return print(f"NoneType exist! Please process None firstly!")
                        aggregate_value_2 = self._aggregate_col(agg_function_right, agg_col_vals_2)

                        boo1 = self._comparison(operator_left, aggregate_value_1, target_value_left)
                        boo2 = self._comparison(operator_right, aggregate_value_2, target_value_right)

                        if boo1 or boo2:
                            sub_table_id = id(sub_table)
                            temp_set.add(sub_table_id)
                            rows_filtered_by_having = [sub_table for sub_table in grouped_table if
                                                       id(sub_table) in temp_set]
        return rows_filtered_by_having

    def _order_by(self, cur_table, order_by_clause):
        if order_by_clause:
            sort_keys = []
            descending = []

            # 假设 order_by_clause 直接是从 SQL 解析得到的 ORDER BY 子句内容
            # 例如: "name DESC, age ASC"
            components = order_by_clause.split(',')

            for component in components:
                parts = component.strip().split()
                sort_key = parts[0]
                sort_keys.append(sort_key)

                # 检查是否指定了 ASC 或 DESC，没有则默认为 ASC
                if len(parts) > 1:
                    sort_order = parts[1].upper()
                    descending.append(sort_order == 'DESC')
                else:
                    descending.append(False)  # 默认为 ASC，即 reverse=False

            # 注意逆序处理排序键，以保持排序的稳定性
            for key, desc in reversed(list(zip(sort_keys, descending))):
                cur_table = sorted(cur_table, key=lambda x: x[key], reverse=desc)
        return cur_table

    def _select(self, table, select_col):  # list of dict
        col_names = select_col.replace(" ", '').split(',')  # list of strings

        if len(col_names) == 1 and col_names[0] == '*':
            return table

        agg_col_dict = {}  # {col_name: agg} {col_name:0}
        for col in col_names:
            pattern = r'(\w+)\((\w+)\)'
            matches = re.findall(pattern, col)

            if matches:
                agg, c = matches[0]
                agg_col_dict[c] = agg
            else:
                agg_col_dict[col] = 0

        columns_list = list(agg_col_dict.keys())
        filter_table = [{key: row[key] for key in columns_list if key in row} for row in table]

        agg_col_name = None
        agg_func_name = None
        for k, v in agg_col_dict.items():
            if v != 0:  # need aggregation
                agg_col_name = k
                agg_func_name = v

        if agg_col_name is None:
            return filter_table
        else:
            col_vals = [row[agg_col_name] for row in filter_table if agg_col_name in row]  # col这个column对应的列的数据
            if self._validate_none_in_col_val(col_vals):
                return print(f"NoneType exist! Please process None firstly!")
            agg_id_dict = self._aggregate_col_select(agg_func_name, col_vals)
            result_table = []
            if agg_id_dict is not None:
                key = next(iter(agg_id_dict))
                value = agg_id_dict[key]
                if value == -1:
                    result_dict = {}
                    dict_key = agg_func_name + "(" + agg_col_name + ")"
                    dict_value = key
                    result_dict[dict_key] = dict_value
                    for column_name in agg_col_dict.keys():
                        if column_name == agg_col_name:
                            continue
                        selected_col_vals = [row[column_name] for row in filter_table if column_name in row]
                        if len(set(selected_col_vals)) == 1:
                            result_dict[column_name] = next(iter(set(selected_col_vals)))
                        else:
                            print(f"Contains no aggregated column which is not functionally dependent on columns in GROUP BY clause!")
                            return
                    result_table.append(result_dict)
                else:
                    for id in value:
                        dict_key = agg_func_name + "(" + agg_col_name + ")"
                        filter_table[id][dict_key] = filter_table[id].pop(agg_col_name)
                        result_table.append(filter_table[id])
            return result_table

    def _where_filter_by_index(self, table, where_clause):
        filtered_rows = []
        if where_clause is None:  # 没有where
            # 创建一个空列表来存储字典
            # 使用items()方法遍历BTree中的所有项
            for key, v in table['data'].items():
                # 每个键值对转换为字典，并添加到列表中
                filtered_rows.append(v)
        else:  # 有where
            parser = Parser(where_clause)
            conditions, logic_clause = parser.parse_clause()
            # Conjunctive optimization
            if len(conditions) == 1:
                filtered_rows = self._filter_rows_by_index(conditions[0], table)
            else:
                # Index -> List of dictionary
                Index_to_dict_rows = []
                # 使用items()方法遍历BTree中的所有项
                for key, v in table['data'].items():
                    # 每个键值对转换为字典，并添加到列表中
                    Index_to_dict_rows.append(v)
                parser_condition_left = Parser(conditions[0])
                parser_condition_right = Parser(conditions[1])
                column_name_left, op_left, value_left = parser_condition_left.parse_condition()
                column_name_right, op_right, value_right = parser_condition_right.parse_condition()
                if logic_clause == 'AND':
                    if self._set_selectivity(column_name_left, Index_to_dict_rows) < self._set_selectivity(
                            column_name_right, Index_to_dict_rows):
                        first_filter = conditions[0]
                        second_filter = conditions[1]
                    else:
                        first_filter = conditions[1]
                        second_filter = conditions[0]
                    first_filtered_rows = self._filter_rows_by_index(first_filter, table)
                    filtered_rows = self._filter_rows(second_filter, first_filtered_rows)
                else:
                    filtered_rows_0 = self._filter_rows_by_index(conditions[0], table)
                    filtered_rows_1 = self._filter_rows_by_index(conditions[1], table)
                    # 使用集合来存储唯一的字典
                    seen = set()
                    # 结果列表
                    unique_dicts = []
                    # 合并两个列表
                    combined_list = filtered_rows_0 + filtered_rows_1
                    # 遍历合并后的列表
                    for d in combined_list:
                        # 将字典转换为可哈希的元组形式
                        dict_tuple = tuple(sorted(d.items()))
                        # 检查是否已在集合中
                        if dict_tuple not in seen:
                            seen.add(dict_tuple)
                            unique_dicts.append(d)
                    filtered_rows = unique_dicts
        return filtered_rows

    def create_database(self):
        parser = Parser(self.sql)
        result = parser.parser_create_database()
        database_name = result['database_name']
        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path)
            print(f"Database '{database_name}' created successfully!")
            return
        else:
            print(f"Database '{database_name}' already exists.")
            return

    def create_table(self):
        parser = Parser(self.sql)
        result = parser.parser_create_table()
        table_name = result['table_name']
        columns = result['columns']
        pk = result['primary_key']
        table_index = OOBTree()
        pk_index = OOBTree()

        table = {
            "columns": columns,
            "primary_key": pk,
            "foreign_keys": result['foreign_keys'],
            "data": table_index,
            "indexes": {
                result['primary_key']: pk_index
            }
        }
        print(table)
        # Check whether referred table exist
        if not self._validate_foreign_key(table['foreign_keys']):
            return
        reference_table_list = {}
        file_path = os.path.join(self.directory_path, 'reference_table_list.json')
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                reference_table_list = json.load(file)
        for k, v in result['foreign_keys'].items():
            if v not in reference_table_list:
                reference_table_list[v] = {}
            reference_table_list[v][table_name] = k
        with open(file_path, 'w') as file:
            json.dump(reference_table_list, file, indent=4)
        # Check whether table exists
        # If not, create a new json file for table
        json_file_path = os.path.join(self.directory_path, table_name + '.json')
        if not os.path.exists(json_file_path):
            self.fileConverter.write_to_json(json_file_path, table)
            print(f"Table '{table_name}' created successfully!")
            return
        else:
            print(f"Table '{table_name}' already exists.")
            return

    def create_index(self):
        parser = Parser(self.sql)
        result = parser.parser_create_index()
        table_name = result['table_name']
        column_name = result['column_name']
        # Check whether table is in database
        json_file_path = os.path.join(self.directory_path, table_name + '.json')
        if not json_file_path:
            print(f"{table_name} does not exist! Please input the right table.")
            return
        table = self.fileConverter.read_from_json(json_file_path)
        pk = table['primary_key']
        columns = table['columns']
        # Check whether column is in table
        if column_name not in columns:
            print(f"Create index failed! There is no {column_name} in {table_name}!")
            return
        # Check whether index has existed
        if column_name in table['indexes']:
            print(f"Create index failed! Index for {column_name} exists!")
            return
        column_tree = OOBTree()
        for key, value in table['data'].items():
            if value[column_name] in column_tree:
                column_tree[value[column_name]].append(value[pk])
            else:
                column_tree[value[column_name]] = [value[pk]]
        table['indexes'][column_name] = column_tree
        self.fileConverter.write_to_json(json_file_path, table)
        print(f"create index for{column_name} successfully!")
        return

    def _insert_record_to_table(self, table_name, records):
        # Get file path of table_name.json
        json_file_path = os.path.join(os.getcwd(), self.database_name, f"{table_name}.json")
        if not os.path.exists(json_file_path):
            print(f"Table {table_name} does not exit!")
            return
        # Read and Parse JSON file
        table = self.fileConverter.read_from_json(json_file_path)

        # Get data of inserted table
        columns = table['columns']
        primary_key = table['primary_key']
        foreign_keys_dict = table['foreign_keys']
        data = table['data']
        indexes = table['indexes']

        # Get data from reference table of each foreign key
        refer_table_dict = {}
        for foreign_ley, refer_table_name in foreign_keys_dict.items():
            json_file_path_re = os.path.join(os.getcwd(), self.database_name, f"{refer_table_name}.json")
            refer_table = self.fileConverter.read_from_json(json_file_path_re)
            refer_table_dict[foreign_ley] = refer_table['data']

        # Insert records into table
        for record in records:
            pk_record = record[primary_key]
            # Check duplicated primary key
            if pk_record in data:
                print(f"Duplicated primary key {pk_record} is not allowed!")
                return
            data[pk_record] = {}
            for column, col_type in columns.items():
                # Check foreign key
                if column in foreign_keys_dict:
                    refer_table_data = refer_table_dict[column]
                    if record[column] not in refer_table_data:
                        print(f"The value of foreign key doesn't exist in referred table."
                              f"Insert failed!")
                        return
                # Value of column is None
                if column not in record:
                    data[pk_record][column] = None
                else:
                    if col_type.upper() == 'INT':
                        data[pk_record][column] = int(record[column])
                    else:
                        data[pk_record][column] = record[column]
        table['data'] = data

        # Insert index if exist
        for record in records:
            pk_record = record[primary_key]
            for column_name, value in record.items():
                if column_name in indexes:
                    column_tree = indexes[column_name]
                    if value in column_tree:
                        indexes[column_name][value].append(pk_record)
                    else:
                        indexes[column_name][value] = [pk_record]
        table['indexes'] = indexes

        # Write data into json file
        try:
            self.fileConverter.write_to_json(json_file_path, table)
            print(f"Insert records into {table_name} successfully!")
            return
        except Exception as e:
            print(f"Failed to write data to JSON file: {e}")
            return

    def insert_records_from_interface(self):
        parser = Parser(self.sql)
        result = parser.parser_insert()
        table_name = result['table_name']
        records = result['records']
        self._insert_record_to_table(table_name, records)
        return

    def import_file(self, filename):
        # Read CSV file and return list of dictionaries
        table_name, _ = os.path.splitext(filename)
        json_file_path = os.path.join(os.getcwd(), self.database_name, f"{table_name}.json")
        table = self.fileConverter.read_from_json(json_file_path)
        columns = table['columns']
        data = []
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                for key, value in row.items():
                    if columns[key].upper() == 'INT':
                        row[key] = int(value)
                data.append(row)
        self._insert_record_to_table(table_name, data)
        return

    def select_from_table(self):
        parser = Parser(self.sql)
        result = parser.parser_select()
        # Get table_a data
        table_a_name = result['from']
        json_table_a_path = os.path.join(os.getcwd(), self.database_name, f"{table_a_name}.json")
        data_a = self.fileConverter.read_from_json(json_table_a_path)

        # Get table_b data
        table_b_name = result['join']
        if table_b_name is not None:
            joined_rows = []
            json_table_b_path = os.path.join(os.getcwd(), self.database_name, f"{table_b_name}.json")
            data_b = self.fileConverter.read_from_json(json_table_b_path)

            # Split "left = right"
            on_condition = result['on']
            parts = on_condition.split('=')
            parser_left = Parser(parts[0])
            parser_right = Parser(parts[1])
            left_table_name, left_col = parser_left.parse_column_relationship()
            right_table_name, right_col = parser_right.parse_column_relationship()
            # Join optimization
            table_a_rows = data_a['data']
            table_b_rows = data_b['data']
            table_a_list = []
            table_b_list = []
            long_list = []
            short_list = []
            for key_a, value_a in table_a_rows.items():
                table_a_list.append(value_a)
            for key_b, value_b in table_b_rows.items():
                table_b_list.append(value_b)
            sort_merge_selection = False
            if len(table_b_list) < len(table_a_list):
                if 10 * len(table_b_list) < len(table_a_list):
                    long_list = table_a_list
                    short_list = table_b_list
                    sort_merge_selection = True
            else:
                if 10 * len(table_a_list) < len(table_b_list):
                    long_list = table_b_list
                    short_list = table_a_list
                    sort_merge_selection = True
            if sort_merge_selection: # Select sort-merged join
                if left_table_name == table_a_name:
                    key1 = left_col
                    key2 = right_col
                else:
                    key1 = right_col
                    key2 = left_col
                sorted_table1 = sorted(long_list, key=lambda x: x[key1])
                sorted_table2 = sorted(short_list, key=lambda x: x[key2])
                # 初始化指针
                i, j = 0, 0

                # 遍历两个已排序的表
                while i < len(sorted_table1) and j < len(sorted_table2):
                    row1 = sorted_table1[i]
                    row2 = sorted_table2[j]

                    # 当找到匹配的键时
                    if row1[key1] == row2[key2]:
                        # 合并匹配的行
                        merged_row = {**row1, **row2}
                        joined_rows.append(merged_row)

                        # 移动指针
                        i += 1
                    else:
                        j += 1

            else: # Select nested-loop join
                if len(table_b_rows) < len(table_a_rows):
                    outer_table = table_b_rows
                    outer_col = left_col
                    inner_table = table_a_rows
                    inner_col = right_col
                    inner_data = data_a
                else:
                    outer_table = table_a_rows
                    outer_col = right_col
                    inner_table = table_b_rows
                    inner_col = left_col
                    inner_data = data_b

                indexes_list = inner_data['indexes']
                # Check whether inner_col is index of inner_table
                if inner_col in indexes_list:
                    inner_index = indexes_list[inner_col]
                else:
                    inner_index = None

                for key, outer_row in outer_table.items():
                    if inner_index:
                        # If inner_index exists, use index to match records
                        if outer_row[outer_col] in inner_index:
                            pk_list = inner_index[outer_row[outer_col]]
                            for pk in pk_list:
                                joined_row = inner_table[pk]
                                joined_row.update({f"{key}": value for key, value in outer_row.items() if
                                                   key != inner_col})
                                joined_rows.append(joined_row)
                    else:
                        for k, inner_row in inner_table.items():
                            if outer_row[outer_col] == inner_row[inner_col]:
                                joined_row = {f"{key}": value for key, value in outer_row.items()}
                                joined_row.update({f"{key}": value for key, value in inner_row.items() if
                                                   key != inner_col})
                                joined_rows.append(joined_row)
            # Where
            where_clause = result['where']
            if where_clause is None:
                final_filtered_rows = joined_rows
            else:
                parser = Parser(where_clause)
                conditions, logic_clause = parser.parse_clause()
                # Conjunctive optimization
                if len(conditions) == 1:
                    final_filtered_rows = self._filter_rows(conditions[0], joined_rows)
                else:
                    parser_condition_left = Parser(conditions[0])
                    parser_condition_right = Parser(conditions[1])
                    column_name_left, op_left, value_left = parser_condition_left.parse_condition()
                    column_name_right, op_right, value_right = parser_condition_right.parse_condition()
                    if logic_clause == 'AND':
                        if self._set_selectivity(column_name_left, joined_rows) < self._set_selectivity(column_name_right, joined_rows):
                            first_filter = conditions[0]
                            second_filter = conditions[1]
                        else:
                            first_filter = conditions[1]
                            second_filter = conditions[0]
                        first_filtered_rows = self._filter_rows(first_filter, joined_rows)
                        final_filtered_rows = self._filter_rows(second_filter, first_filtered_rows)
                    else:
                        filtered_rows_0 = self._filter_rows(conditions[0], joined_rows)
                        filtered_rows_1 = self._filter_rows(conditions[1], joined_rows)
                        # 使用集合来存储唯一的字典
                        seen = set()
                        # 结果列表
                        unique_dicts = []
                        # 合并两个列表
                        combined_list = filtered_rows_0 + filtered_rows_1
                        # 遍历合并后的列表
                        for d in combined_list:
                            # 将字典转换为可哈希的元组形式
                            dict_tuple = tuple(sorted(d.items()))
                            # 检查是否已在集合中
                            if dict_tuple not in seen:
                                seen.add(dict_tuple)
                                unique_dicts.append(d)
                        final_filtered_rows = unique_dicts
        else: # 没有join on
            where_clause = result['where']
            final_filtered_rows = self._where_filter_by_index(data_a, where_clause)

        # Execute group by and having
        group_column = result['group_by']
        having_clause = result['having']
        select_col = result["select"]
        group_by_having_rows = []
        selected_rows = []
        if not group_column:
            group_by_having_rows = final_filtered_rows
            selected_rows = self._select(group_by_having_rows, select_col)
        else:
            target_col_list = [group_column]
            group_by_having_list = self._group_by_having(final_filtered_rows, target_col_list, having_clause)
            for subList in group_by_having_list:
                selected_subList = self._select(subList, select_col)
                if selected_subList is None:
                    return
                for sub_selected in selected_subList:
                    selected_rows.append(sub_selected)

        # Execute order by
        order_by_clause = result['order_by']
        order_by_table = self._order_by(selected_rows, order_by_clause)

        return order_by_table

    def update_table(self):
        parser = Parser(self.sql)
        result = parser.parser_update()
        table_name = result['update']
        set_clause = result['set']
        where_clause = result['where']

        # Read json file to table data
        json_file_path = os.path.join(os.getcwd(), self.database_name, f"{table_name}.json")
        table = self.fileConverter.read_from_json(json_file_path)

        # Filter rows where_clause
        filtered_rows = self._where_filter_by_index(table, where_clause)

        # Update according to set clause
        parser = Parser(set_clause)
        col, op, value = parser.parse_condition()
        foreign_keys = table['foreign_keys']

        # Check whether col updated is primary key
        primary_key = table['primary_key']
        if primary_key == col:
            print(f"{col} is primary key. Please update it carefully!")
            return

        # Column updated is not foreign key
        if col in foreign_keys:
            refer_table_name = foreign_keys[col]
            json_file_refer_path = os.path.join(os.getcwd(), self.database_name, f"{refer_table_name}.json")
            refer_table = self.fileConverter.read_from_json(json_file_refer_path)
            refer_table_data = refer_table['data']
            if value not in refer_table_data:
                print(f"Update failed!"
                      f"You are updating a foreign key. The value is not in reference table!")
                return

        # Column updated is an index
        if col in table['indexes']:
            tree = table['indexes'][col]
            for row in filtered_rows:
                if row[col] in tree:
                    old_value = row[col]
                    if value in tree:
                        # If value is in Btree, merge two lists
                        tree[value].extend(tree.pop(old_value))
                    else:
                        tree[value] = tree.pop(old_value)
        # Update record
        for row in filtered_rows:
            row[col] = value
        # Update table to json file
        try:
            self.fileConverter.write_to_json(json_file_path, table)
            print(f"Update {table_name} successfully!")
            return
        except Exception as e:
            print(f"Failed to write data to JSON file: {e}")
            return

    def delete_table(self):
        parser = Parser(self.sql)
        result = parser.parser_delete()
        table_name = result['from']
        where_clause = result['where']
        json_file_path = os.path.join(os.getcwd(), self.database_name, f"{table_name}.json")
        table = self.fileConverter.read_from_json(json_file_path)
        file_path = os.path.join(self.directory_path, 'reference_table_list.json')
        with open(file_path, 'r') as file:
            reference_table_list = json.load(file)
        # filter rows by where_clause
        filtered_rows = self._where_filter_by_index(table, where_clause)
        # Get deleted pk list
        pk_list = []
        primary_key = table['primary_key']
        for filtered_row in filtered_rows:
            pk = filtered_row[primary_key]
            pk_list.append(pk)
        # Check whether table is a reference table
        if table_name in reference_table_list:
            for re_table_name, fk_name in reference_table_list[table_name].items():
                fk_list = []
                json_re_file_path = os.path.join(os.getcwd(), self.database_name, f"{re_table_name}.json")
                re_table = self.fileConverter.read_from_json(json_re_file_path)
                for id, row in re_table['data'].items():
                    fk_value = row[fk_name]
                    fk_list.append(fk_value)
                duplicates = list(filter(lambda x: x in pk_list, fk_list))
                if duplicates:
                    print("Deleted row is related with referred foreign key. Delete failed！")
                    return
        for pk in pk_list:
            table['data'].pop(pk)
        print("Successfully delete rows!")

        # Delete index
        indexes = table['indexes']
        for filtered_row in filtered_rows:
            pk_value = filtered_row[primary_key]
            for k, v in filtered_row.items():
                if k in indexes:
                    # Delete pk_value in indexes[k][v], indexes[k][v] is a list
                    indexes[k][v].remove(pk_value)
                    # If the list is None, delete the list
                    if not indexes[k][v]:
                        del indexes[k][v]
        print("Successfully delete indexes")

        # Write deleted table into json file
        try:
            self.fileConverter.write_to_json(json_file_path, table)
            print(f"DELETE {table_name} successfully!")
            return
        except Exception as e:
            print(f"Failed to write data to JSON file: {e}")
            return

    def drop_table(self):
        parser = Parser(self.sql)
        result = parser.parser_drop()
        table_name = result['table_name']
        re_file_path = os.path.join(self.directory_path, 'reference_table_list.json')
        with open(re_file_path, 'r') as file:
            reference_table_dict = json.load(file)
        if table_name in reference_table_dict:
            print(f"{table_name} is a reference table. Please delete the foreign key firstly!")
        else:
            file_path = os.path.join(self.directory_path, f"{table_name}.json")
            if os.path.isfile(file_path):
                os.remove(file_path)
            else:
                print(f"{table_name} dose not exist!")
            reference_table_dict_copy = reference_table_dict.copy()
            for reference_table, fk_tables in reference_table_dict_copy.items():
                fk_tables_copy = fk_tables.copy()
                for fk_table, fk in fk_tables_copy.items():
                    if fk_table == table_name:
                        del fk_tables[fk_table]
                if len(fk_tables) == 0:
                    del reference_table_dict[reference_table]
            self.fileConverter.write_to_json(re_file_path, reference_table_dict)
            print(f"{table_name} has been deleted.")


