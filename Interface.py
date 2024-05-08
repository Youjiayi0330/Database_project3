from Executor import Executor
import sqlparse
import os
import time

def handle_sql(sql, database_name):
    sql = sql.strip()
    sql_parsed = sqlparse.parse(sql)[0]
    sql_tokens = sql_parsed.tokens
    executor = Executor(sql, database_name)
    if sql_tokens[0].value.upper() == 'CREATE':
        if sql_tokens[2].value.upper() == 'DATABASE':
            result = executor.create_database()
        elif sql_tokens[2].value.upper() == 'TABLE':
            result = executor.create_table()
        elif sql_tokens[2].value.upper() == 'INDEX':
            result = executor.create_index()
        else:
            print('Syntax Error! Please enter again!')
            return
    elif sql_tokens[0].value.upper() == 'INSERT':
        result = executor.insert_records_from_interface()
    elif sql_tokens[0].value.upper() == 'SELECT':
        result = executor.select_from_table()
    elif sql_tokens[0].value.upper() == 'UPDATE':
        result = executor.update_table()
    elif sql_tokens[0].value.upper() == 'DELETE':
        result = executor.delete_table()
    elif sql_tokens[0].value.upper() == 'DROP':
        result = executor.drop_table()
    elif sql.upper().startswith("IMPORT"):
        parts = sql.split()
        filename = parts[1]
        result = executor.import_file(filename)
    else:
        raise ValueError('Syntax Error! Please enter again!')
    return result


def visualization(data):
    if len(data) == 0:
        print(data)
        return
    columns = list(data[0].keys())
    col_width = {col: len(col) for col in columns}  # Start with the length of the column names

    for item in data:
        for col in columns:
            col_width[col] = max(col_width[col], len(str(item[col])))

            # Step 2: Print the header
    header = " | ".join(col.ljust(col_width[col]) for col in columns)
    print(header)
    print('-' * len(header))

    # Step 3: Print the rows
    for item in data:
        row = " | ".join(str(item[col]).ljust(col_width[col]) for col in columns)
        print(row)


def main():
    database_name = None
    access = False
    while(True):
        user_input = input("Please enter sql statement: ")
        sql_parsed = sqlparse.parse(user_input)[0]
        sql_tokens = sql_parsed.tokens
        if sql_tokens[0].value.upper() == 'USE':
            database_name = sql_tokens[2].value
            database_path = os.path.join(os.getcwd(), database_name)
            if os.path.isdir(database_path):
                print(f"Enter '{database_name}' database successfully!")
                access = True
                continue
            else:
                print(f"Database '{database_name}' does not exist.")
                continue
        if access:
            start = time.time()
            result = handle_sql(user_input, database_name)
            end = time.time()
            if result is not None:
                visualization(result)
            execution_time = end - start
            print(f"Execution time: {execution_time}")
        else:
            print("Please firstly enter one database!")



if __name__ == '__main__':
    main()