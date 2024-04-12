import re


class Executor:
    def __init__(self, data):
        self.data = data

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

    def execute(self, query):
        # Get raw rows
        table_name = query["from"]
        if table_name not in self.data:
            raise ValueError("Table does not exist")
        raw_rows = self.data[table_name]

        rows = self._select_execute(query["select"], rows_after_X)