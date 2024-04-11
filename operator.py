from BTrees.OOBTree import OOBTree
from BTrees.OIBTree import OIBTree

# Read file
tables = {}
constraints = {}


# Create table
def create_table (table_name, primary_key, foreign_key, ref_table):
    tables[table_name] = [record1, record2, ...] ## record = {'id': , ...}
    constraints[table_name] = {
        'primary_key' : primary_key,
        'foreign_tuple' : (foreign_key, ref_table)
    }


# Insert data into county_table, owner_table, type_table
# Create Index
def insert_table_btree(table_name, name):
    name_index = OOBTree() # String

    table = tables[table_name]

    # BTree index for name
    for data in table:
        name_index[data[name]] = data

def insert_table_tries_btree(table_name):
    table = tables[table_name]
    county_id_index = OIBTree()



def insert_table_hospital_overview(table_name):
    """
    # 插入数据到Trie中
    hospital_id_trie = pygtrie.CharTrie()
    hospital_id_trie[hospital_id] = data
    """

def main():
    # Read file