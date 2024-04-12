import re

# sql_query = """
# SELECT
#     c.county,
#     t.type,
#     o.owner_name,
#     COUNT(h.hospital_id) AS hospital_count,
#     MAX(h.beds) AS max_beds,
#     MIN(h.beds) AS min_beds
# FROM
#     hospital_overview h
# JOIN
#     hospital_county hc ON h.hospital_id = hc.hospital_id
# WHERE
#     c.county LIKE 'B%' AND (h.beds > 200 OR h.helipad = 'Y')
# GROUP BY
#     c.county, t.type, o.owner_name
# ORDER BY
#     c.county ASC;
# """


class Parser:
    def __init__(self, sql_query):
        self.sql_query = sql_query

    def _statement_to_string(self, statement):
        if statement is None:
            return statement
        else:
            return re.sub(r'\s*,\s*', ',', statement.group(1).replace('\n', ' ')).strip()

    def parser(self):
        # Match SELECT
        select_statement = re.search(r'SELECT(.*?)FROM', self.sql_query, re.DOTALL)
        select = self._statement_to_string(select_statement)
        print("SELECT:", select)

        # Match FROM
        from_statement = re.search(r'FROM(.*?)(?=JOIN|WHERE|GROUP BY|ORDER BY|$)', self.sql_query, re.DOTALL)
        from_st = self._statement_to_string(from_statement)
        print("FROM:", from_st)

        # Match JOIN
        join_statement = re.search(r'JOIN(.*?)ON', self.sql_query, re.DOTALL)
        join = self._statement_to_string(join_statement)
        print("JOIN:", join)

        # Match ON
        on_statement = re.search(r'ON(.*?)(?=WHERE|GROUP BY|ORDER BY|$)', self.sql_query, re.DOTALL)
        on = self._statement_to_string(on_statement)
        print("ON:", on)

        # Match WHERE
        where_statement = re.search(r'WHERE(.*?)(?=GROUP BY|ORDER BY|$)', self.sql_query, re.DOTALL)
        where = self._statement_to_string(where_statement)
        print("WHERE:", where)

        # Match GROUP BY
        group_by_statement = re.search(r'GROUP BY(.*?)(?=HAVING|ORDER BY|$)', self.sql_query, re.DOTALL)
        group_by = self._statement_to_string(group_by_statement)
        print("GROUP BY:", group_by)

        # Match HAVING
        having_statement = re.search(r'HAVING(.*?)(?=ORDER BY|$)', self.sql_query, re.DOTALL)
        having = self._statement_to_string(having_statement)
        print("HAVING:", having)

        # Match ORDER BY
        order_by_statement = re.search(r'ORDER BY(.*?);', self.sql_query, re.DOTALL)
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
