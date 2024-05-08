import sqlparse
import re

class Parser:
    def __init__(self, sql_command):
        self.sql_command = sql_command

    def _parse_sql_command(self):
        parsed = sqlparse.parse(self.sql_command)[0]
        tokens = parsed.tokens
        return tokens

    def _statement_to_string(self, statement):
        if statement is None:
            return statement
        else:
            return re.sub(r'\s*,\s*', ',', statement.group(1).replace('\n', ' ')).strip()

    def parse_column_relationship(self):
        clean_part = self.sql_command.strip()
        table, column = clean_part.split('.')
        return table, column

    def parse_clause(self):
        pattern = re.compile(r'(\b(?:SUM|MAX|MIN|AVG|COUNT)\s*\(\w+\)\s*[=<>!]{1,2}\s*\d+|\w+\s*[=<>!]{1,2}\s*(?:"[^"]*"|\'[^\']*\'|\d+))\s*(AND|OR|$)?', re.IGNORECASE)
        matches = pattern.findall(self.sql_command)
        conditions = [] # list of conditions: {condition_a, condition_b}
        logic_clause = None # {AND, OR}
        for match in matches:
            conditions.append(match[0].strip())
            if match[1]:
                logic_clause = match[1]
        return conditions, logic_clause

    def parse_condition(self):
        pattern = re.compile(r'(\w+)\s*([=!<>]{1,2}|LIKE)\s*(\d+|"[^"]*"|\'[^\']*\')', re.IGNORECASE)
        match = pattern.match(self.sql_command.strip())  # 使用match确保从头开始匹配
        if match:
            column_name = match.group(1)  # 字段名
            operator = match.group(2)  # 操作符
            value = match.group(3)  # 值
            # 去除可能的引号
            if value.startswith(('"', "'")) and value.endswith(('"', "'")):
                value = value[1:-1]
            else:
                value = int(value)
            return column_name, operator, value
        else:
            return {'error': 'SQL syntax error!'}

    def parser_create_database(self):
        tokens = self._parse_sql_command()
        database_name = tokens[4].value
        return {"database_name": database_name}

    def parser_create_table(self):
        table_name = re.search(r"CREATE TABLE (\w+)", self.sql_command).group(1)
        columns = re.findall(r"(\w+) (\w+\(?\d*\)?),", self.sql_command)
        primary_key = re.search(r"PRIMARY KEY \((\w+)\)", self.sql_command).group(1)
        foreign_keys = re.findall(r"FOREIGN KEY \((\w+)\) REFERENCES (\w+)\(\w+\)", self.sql_command)

        result = {
            "table_name": table_name,
            "columns": {name: type_ for name, type_ in columns},
            "primary_key": primary_key,
            "foreign_keys": {fk[0]: fk[1] for fk in foreign_keys} # {foriegn_key: reference_table, ...}
        }
        return result

    def parser_create_index(self):
        pattern = r"CREATE INDEX \w+ ON (\w+) \((\w+)\);"
        match = re.search(pattern, self.sql_command)
        if match:
            table_name = match.group(1)
            index_column = match.group(2)
            result = {'table_name': table_name, 'column_name': index_column}
            return result
        else:
            return {'error': 'SQL syntax error!'}

    def parser_insert(self):
        match = re.search(r"INSERT INTO\s+(\w+)\s*\((.*?)\)\s*VALUES", self.sql_command, re.IGNORECASE)

        if match:
            table_name = match.group(1)
            columns = match.group(2).split(", ")
            # 修正了values的匹配，确保正确地从VALUES语句中获取值列表
            values_part = self.sql_command[match.end():].strip()
            values = re.findall(r"\((.*?)\);?", values_part, re.IGNORECASE)
            records = []
            for value in values:
                # 正确地分割值，考虑到了字符串中的逗号
                value_list = [v.strip() for v in re.split(r",\s*(?=(?:[^']*'[^']*')*[^']*$)", value)]
                record = {}
                for col, val in zip(columns, value_list):
                    # 去除值两侧可能的引号
                    cleaned_val = val.strip("'")
                    # 判断值是否为数字并相应地转换
                    if cleaned_val.isdigit():
                        cleaned_val = int(cleaned_val)
                    record[col.strip()] = cleaned_val
                records.append(record)

            result = {
                'table_name': table_name,
                'records': records
            }
        return result

    def parser_select(self):
        # Match SELECT
        select_statement = re.search(r'SELECT(.*?)FROM', self.sql_command, re.DOTALL)
        select = self._statement_to_string(select_statement)
        print("SELECT:", select)

        # Match FROM
        from_statement = re.search(r'FROM\s+(.*?)(?=JOIN|WHERE|GROUP BY|HAVING|ORDER BY|;$)', self.sql_command, re.DOTALL)
        from_st = self._statement_to_string(from_statement)
        print("FROM:", from_st)

        # Match JOIN
        join_statement = re.search(r'JOIN\s+(.*?)(?=ON|WHERE|GROUP BY|HAVING|ORDER BY|;$)', self.sql_command, re.DOTALL)
        join = self._statement_to_string(join_statement)
        print("JOIN:", join)

        # Match ON
        on_statement = re.search(r'ON\s+(.*?)(?=JOIN|WHERE|GROUP BY|HAVING|ORDER BY|;$)', self.sql_command, re.DOTALL)
        on = self._statement_to_string(on_statement)
        print("ON:", on)

        # Match WHERE
        where_statement = re.search(r'WHERE\s+(.*?)(?=GROUP BY|HAVING|ORDER BY|;$)', self.sql_command, re.DOTALL)
        where = self._statement_to_string(where_statement)
        print("WHERE:", where)

        # Match GROUP BY
        group_by_statement = re.search(r'GROUP BY\s+(.*?)(?=HAVING|ORDER BY|;$)', self.sql_command, re.DOTALL)
        group_by = self._statement_to_string(group_by_statement)
        print("GROUP BY:", group_by)

        # Match HAVING
        having_statement = re.search(r'HAVING\s+(.*?)(?=ORDER BY|;$)', self.sql_command, re.DOTALL)
        having = self._statement_to_string(having_statement)
        print("HAVING:", having)

        # Match ORDER BY
        order_by_statement = re.search(r'ORDER BY\s+(.*?);$', self.sql_command, re.DOTALL)
        order_by = self._statement_to_string(order_by_statement)
        print("ORDER BY:", order_by)

        result = {
            "select": select,
            "from": from_st,
            "join": join,
            "on": on,
            "where": where,
            "group_by": group_by,
            "having": having,
            "order_by": order_by
        }

        return result

    def parser_update(self):
        pattern = r"UPDATE\s+(\w+)\s+SET\s+(.+?)\s+WHERE\s+(.+?);"
        match = re.search(pattern, self.sql_command, re.IGNORECASE | re.DOTALL)
        if match:
            update = match.group(1)
            set_expression = match.group(2)
            where_clause = match.group(3)

            result = {
                'update': update,
                'set': set_expression,
                'where': where_clause
            }
            return result
        else:
            return {'error': 'SQL syntax error!'}

    def parser_delete(self):
        pattern = r"DELETE FROM\s+(\w+)\s+WHERE\s+(.+?);"
        match = re.search(pattern, self.sql_command, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            where_clause = match.group(2)
            result = {
                'from': table_name,
                'where': where_clause
            }
            return result
        else:
            return {'error': 'SQL syntax error!'}

    def parser_aggregate(self, condition):
        pattern = r'(\bSUM|\bMAX|\bMIN|\bAVG|\bCOUNT)\s*\(\s*(?:\"(\w+)\"|(\w+))\s*\)\s*(!=|>=|<=|>|<|=)\s*(\d+)'
        match = re.match(pattern, condition)
        if match:
            agg_function = match.group(1)
            distinct_clause = bool(match.group(2))
            agg_column_name = match.group(3)
            operator = match.group(4)
            target_value = match.group(5)
            if target_value.startswith(('"', "'")) and target_value.endswith(('"', "'")):
                target_value = target_value[1:-1]
            else:
                target_value = int(target_value)
            return agg_function, distinct_clause, agg_column_name, operator, target_value
        else:
            raise ValueError("The condition string does not match the expected format.")

    def parser_drop(self):
        pattern = r'^\s*DROP\s+TABLE\s+(\S+)\s*;?$'
        match = re.match(pattern, self.sql_command, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            result = {
                'table_name': table_name
            }
            return result
        else:
            raise ValueError("The SQL statement is not a valid DROP TABLE command.")
