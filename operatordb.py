from BTrees.OIBTree import OIBTree
from BTrees.OOBTree import OOBTree
import pygtrie
import csv


"""
#validate table_name, primary_key and foreign_key
"""
tables = {}
constraints = {}

def validate_table_name(table_name):
    if not isinstance(table_name, str):
        raise ValueError("Table name must be a string.")
    if table_name in tables:
        raise ValueError(f"Table '{table_name}' already exists.")

def validate_primary_key(primary_key, columns):
    if primary_key not in columns:
        raise ValueError(f"Primary key '{primary_key}' must be one of the defined columns.")

def validate_foreign_key(foreign_key, ref_table, tables):
    if ref_table not in tables:
        raise ValueError(f"Referenced table '{ref_table}' does not exist.")
    if foreign_key not in tables[ref_table][0]:
        raise ValueError(f"Foreign key '{foreign_key}' does not exist in the referenced table '{ref_table}'.")

"""
create table
"""
def create_table (table_name, column_names, primary_key, foreign_keys = None):
    # Validate the table name and primary key.
    validate_table_name(table_name)
    validate_primary_key(primary_key, column_names)

    #initialization
    tables[table_name] = [] ## record = {'id': , ...}

    #define constrains
    table_constraints = {
        'column_names': column_names,
        'primary_key': primary_key,
        'foreign_keys': {}
    }

    #check foreign keys
    if foreign_keys:
        for fk_column, ref_table in foreign_keys.items():
            validate_foreign_key(fk_column, ref_table, tables)
            table_constraints['foreign_keys'][fk_column] = ref_table

    constraints[table_name] = table_constraints

"""
# Insert data into county_table, owner_table, type_table
# store tables and their indexes
"""
tables = {}
indexes = {}

def insert_table_btree(table_name, column_name):
    if table_name not in tables:
        raise ValueError(f"Table {table_name} does not exist.")
    
    column_index = OOBTree()

    table = tables[table_name]

    # Populate the BTree index
    for data in table:
        # Assume each `data` is a dictionary where the column name maps to the value
        key = data[column_name]
        if key in column_index:
            # Handle duplicate keys if necessary, e.g., append to a list
            column_index[key].append(data)
        else:
            column_index[key] = [data]  # Store data in a list to handle potential duplicates

    indexes[(table_name, column_name)] = column_index

def insert_table_tries_btree(table_name):
    table = tables[table_name]
    county_id_index = OIBTree()

"""
# apply a trie index to the hospital_id column of a hospital_overview table 
"""
trie_indexes = {}
def insert_table_hospital_overview(data, index_column):

    """
    # 插入数据到Trie中
    hospital_id_trie = pygtrie.CharTrie()
    hospital_id_trie[hospital_id] = data
    """

    column_trie = pygtrie.CharTrie()
    
    for record in data:
        # Convert the index column to a string since tries work with string keys
        key = str(record[index_column])
        # Insert the record into the trie using the column value as the key
        column_trie[key] = record

    return column_trie

def read_data_from_file(filename):
    #Read CSV file and return list of dictionaries
    data = []
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def main():
    hospital_overview_data = read_data_from_file('hospital_overview.csv')
    hospital_id_trie = insert_table_hospital_overview(hospital_overview_data, 'hospital_id')
    hospital_country_data = read_data_from_file('hospital_country.csv')
    hospital_owner_data = read_data_from_file('hospital_owner.csv')
    hospital_type_data = read_data_from_file('hospital_type.csv')
    country = read_data_from_file('country.csv')



if __name__ == '__main__':
    main()   


