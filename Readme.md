# Overview

We create three tables: county, owner, and hospital and import data. The data volume of each table is 2591, 7 and 7621, respectively. There are two foreign keys in the hospital table. ‘owner_id’ refers to ‘owner_id’ in the owner table; ‘county_id’ refers to ‘county_id’ in the county table. In addition to adding indices to the primary keys, indices have also been added to the ‘county_id’ in the hospital table and ‘county’ in the county table. All indices are of the BTree type. Figure 1 shows the table structure. 

![img](https://lh7-us.googleusercontent.com/L5gQGJiM-FrVe2jQCPwTa_ELSjg8k7tAoMR2J4HAKnUzwXRTsNe0sCfHRD2eE8ZwsXA6yuI87CVWYiUVQ-ZEY3wqVs7QqcSmVcu2qHpZbdTkcJXU1zw7st0Znb_cVvuTW5xz8lwGVn_BjwBoQA3Xk_Y)

​                                                                            Figure 1. Table structure



# Operation Guideline

Run 'Interface.py' Firstly;

All operation commands can be run in Python Console.

Stop 'Interface.py' if you don't want to execute any command.

## Before test

- Firstly enter database *'healthcare'*

```sql
Use healthcare;
```

- Create County table
  When create County table successfully, *County.json* will be created in *'healthcare'* dictionary;
  reference_table_list will also be created since it is County table is the first table to be created

```sql
CREATE TABLE County (county_id int, county varchar(50),PRIMARY KEY (county_id));
```

- Create Owner table
  When create Owner table successfully, *Owner.json* will be created in *'healthcare'* dictionary;

```sql
CREATE TABLE Owner (owner_id int,owner_name varchar(50),PRIMARY KEY (owner_id));
```

- Create Hospital table
  When create Hospital table successfully, *Hospital.json* will be created in *'healthcare'* dictionary;
  If you create Hospital table before County table, it will show **'Reference table dose not exist'**;

```sql
CREATE TABLE Hospital (hospital_id INT, name VARCHAR(50),population INT, bed INT, county_id INT, owner_id INT, PRIMARY KEY (hospital_id),FOREIGN KEY (county_id) REFERENCES County(county_id),FOREIGN KEY (owner_id) REFERENCES Owner(owner_id));
```

- Import data to County table

```sql
IMPORT County.csv
```

- Import data to Owner table

```sql
IMPORT Owner.csv
```

- Import data to Hospital table

```sql
IMPORT Hospital.csv
```



## Test statements

- Create index

```sql
CREATE INDEX county_name_idx ON County (county);
CREATE INDEX ho_county_id_index ON Hospital (county_id);
```

- Insert records

```sql
INSERT INTO Hospital (hospital_id, name, population, bed, county_id, owner_id) VALUES (100, 'SantPaul', 200, 123, 1, 0);
```

It will show that **'The value of foreign key doesn't exist in referred table.Insert failed!'**

You need to insert record to referenced table firstly. Then, you can insert record into child table again.

```sqlite
INSERT INTO County (county_id, county) VALUES (2, 'Shelbyville'), (3, 'Ogdenville'), (4, 'North Haverbrook');
INSERT INTO Hospital (hospital_id, name, population, bed, county_id, owner_id) VALUES (100, 'SantPaul', 200, 123, 2, 0);
```

The records above can be inserted successfully.

- Select

```sql
1. SELECT COUNT(county_id) FROM County;
2. SELECT * FROM Hospital JOIN County ON Hospital.county_id = County.county_id WHERE county = "Shelbyville";
3. SELECT * FROM Hospital JOIN County ON Hospital.county_id = County.county_id WHERE county = "LAKE" AND bed < 100;
4. SELECT * FROM Hospital JOIN County ON Hospital.county_id = County.county_id WHERE county = "LAKE" OR bed < 100;
5. SELECT AVG(population), owner_id FROM Owner JOIN Hospital ON Owner.owner_id = Hospital.owner_id GROUP BY owner_id;
6. SELECT MIN(population), name, bed FROM County JOIN Hospital ON  County.county_id= Hospital.county_id GROUP BY county_id ORDER BY bed DESC;
7. SELECT MAX(bed), name, county, owner_id FROM County JOIN Hospital ON County.county_id = Hospital.county_id WHERE county = 'LAKE' GROUP BY owner_id HAVING AVG(population) > 0 ORDER BY name;
8. SELECT SUM(bed), owner_name FROM Owner JOIN Hospital ON Owner.owner_id = Hospital.owner_id GROUP BY owner_id;
9. SELECT SUM(bed), hospital_id FROM Owner JOIN Hospital ON Owner.owner_id = Hospital.owner_id GROUP BY owner_id;
```

When testing the No.9 select command, it will show **"Contains no aggregated column which is not functionally dependent on columns in GROUP BY clause!"**

- Update

```sql
UPDATE Hospital SET county_id = 5 WHERE name = 'DMC';
```

It will show **"Update failed!You are updating a foreign key. The value is not in reference table!"**

```sql
UPDATE Hospital SET population = 200 WHERE hospital_id = 4;
```

The record can be updated successfully.

- Delete

```sql
DELETE FROM Owner WHERE owner_id = 1;
```

It will show **"Deleted row is related with foreign key. Delete failed"**

```sql
DELETE FROM Hospital WHERE owner_id = 6;
```

The records can be deleted successfully.

- Drop

  Drop County table

```sql
DROP TABLE County
```

It will show **"County is a referenced table. Please delete the foreign key firstly!"**

```sql
DROP TABLE Hospital
DROP TABLE County
DROP TABLE Owner
```

Three tables can be dropped successfully.



# File Outline

- Healthcare
  It is a dictionary. After County, Owner, Hospital tables created, County.json, Owner.json, Hospital.json, reference_table_list.json will also be created.

- County.csv
  2588 records for County table.

- Hospital.csv
  7621 records for Hospital table.

- Owner.csv
  7 records for Owner table

- Executor.py

  Main engine to simulate a SQL-like database management system.

- Parser.py
  Parse sql commands

- FileConverter.py
  Write data from memory to json file; Read json file to memory.

- Interface.py
  Run sql command.

- Statement_demo.txt
  Includes all the test commands.