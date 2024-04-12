import re


class Executor:
    def __init__(self, data):
        self.data = data

    def _parse_column_relationship(self, part):
        clean_part = part.strip()
        table, column = clean_part.split('.')
        if table not in self.data:
            raise ValueError("Table does not exist")
        return table, column

    def _select_execute(self, select_st, rows):
        # Process select
        # Initialize select dict
        select_dict = {
            "column": [],
            "COUNT": None,
            "MAX": None,
            "MIN": None,
            "SUM": None
        }

        # Split select statement
        columns = select_st.split(',')
        for col in columns:
            col = col.strip()
            if '(' in col:
                # Identify aggregation function
                match = re.match(r'(COUNT|MAX|MIN|SUM)\((.+)\)', col)
                if match:
                    func_name = match.group(1)
                    func_arg = match.group(2)
                    select_dict[func_name] = func_arg
            else:
                # field name
                select_dict["column"].append(col)

        # rows filter
        selected_rows = []
        # for row in rows:
        #     selected_row = {column: row[column] for column in columns if column in row}
        #     selected_rows.append(selected_row)
        return selected_rows

    def _join_on_execute(self, table_b_name, on_condition, table_a_rows):
        table_b_rows = self.data[table_b_name]
        joined_rows = []
        # Split "left = right"
        parts = on_condition.split('=')
        left_table_name, left_col = self._parse_column_relationship(parts[0])
        right_table_name, right_col = self._parse_column_relationship(parts[1])
        # join optimization

    # Parse clause into conditions and logic_clause
    def _parse_clause(self, where_clause):
        pattern = re.compile(r'(\w+\.\w+\s*=\s*\'[^\']+\'\s*)(AND|OR|$)?')
        matches = pattern.findall(where_clause)
        conditions = [] # list of conditions: {condition_a, condition_b)
        logic_clause = None # {AND, OR}
        for match in matches:
            conditions.append(match[0].strip())
            if match[1]:
                logic_clause = match[1]

        return conditions, logic_clause

    # Parse one condition into (column_name, op, value)
    def _parse_condition(self, condition):
        pattern = re.compile(r'(\w+\.\w+)\s*(=)\s*\'([^\']+)\'')
        match = pattern.match(condition)
        if match:
            column_name = match.group(1)
            operator = match.group(2)
            value = match.group(3)
            return column_name, operator, value
        else:
            return None

    def _filter_rows(self, condition, rows):



    def _where_execute(self, where_clause, rows):
        conditions, logic_clause = self._parse_clause(where_clause)
        # Conjunctive optimization
        if len(conditions) == 1:
            filtered_rows = self._filter_rows(conditions[0], rows)
        else:
            rows_1 = self._filter_rows(conditions[0], rows)
            rows_2 = self._filter_rows(conditions[1], rows)
            if logic_clause == 'AND':
                if(len(rows_1) > len(rows_2)):
                    filtered_rows = self._filter_rows(rows_1, rows_2)
                else:
                    filtered_rows = self._filter_rows(rows_2, rows_1)
            else
        return filtered_rows

    def execute(self, query):
        # Get raw rows
        table_A_name = query["from"]
        if table_A_name not in self.data:
            raise ValueError("Table does not exist")
        raw_rows = self.data[table_A_name]

        # Join
        table_B_name = query["join"]
        if table_B_name is not None:
            if table_B_name not in self.data:
                raise ValueError("Table does not exist")
            joined_rows = self._join_on_execute(query["join"], query["on"], raw_rows)

        # Where
        where_condition = query["where"]
        if where_condition is not None:
             filtered_rows = self._where_execute(where_condition, joined_rows)

        rows = self._select_execute(query["select"], rows_after_X)