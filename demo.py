import re

sql_query = """
SELECT 
    c.county, 
    t.type, 
    o.owner_name, 
    COUNT(h.hospital_id) AS hospital_count, 
    MAX(h.beds) AS max_beds, 
    MIN(h.beds) AS min_beds
FROM 
    hospital_overview h
JOIN 
    hospital_county hc ON h.hospital_id = hc.hospital_id
JOIN 
    county c ON hc.county_id = c.county_id
JOIN 
    hospital_type t ON h.type_id = t.type_id
JOIN 
    hospital_owner o ON h.owner_id = o.owner_id
WHERE 
    c.county LIKE 'B%' AND (h.beds > 200 OR h.helipad = 'Y')
GROUP BY 
    c.county, t.type, o.owner_name
HAVING 
    COUNT(h.hospital_id) > 2
ORDER BY 
    c.county ASC;
"""

# 匹配 SELECT 语句
select_statement = re.findall(r'SELECT(.*?)FROM', sql_query, re.DOTALL)
print("SELECT 语句:", select_statement)

# 匹配 JOIN 语句
join_statements = re.findall(r'(JOIN.*?)ON', sql_query, re.DOTALL)
print("JOIN 语句:", join_statements)

# 匹配 WHERE 语句
where_statement = re.search(r'WHERE(.*?)GROUP BY', sql_query, re.DOTALL)
print("WHERE 语句:", where_statement.group(1) if where_statement else None)

# 匹配 GROUP BY 语句
group_by_statement = re.search(r'GROUP BY(.*?)HAVING', sql_query, re.DOTALL)
print("GROUP BY 语句:", group_by_statement.group(1) if group_by_statement else None)

# 匹配 HAVING 语句
having_statement = re.search(r'HAVING(.*?)ORDER BY', sql_query, re.DOTALL)
print("HAVING 语句:", having_statement.group(1) if having_statement else None)

# 匹配 ORDER BY 语句
order_by_statement = re.search(r'ORDER BY(.*?);', sql_query, re.DOTALL)
print("ORDER BY 语句:", order_by_statement.group(1) if order_by_statement else None)


result = {
    "SELECT" : [],
    "FROM": TableA,
    "JOIN": TableB,
    "ON": String,
    "WHERE": String,
    "GROUP BY":
    "HAVING":
    "ORDER BY":
}