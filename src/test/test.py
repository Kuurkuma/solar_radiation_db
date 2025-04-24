import duckdb
import sqlite3

duck_conn = duckdb.connect()
#sql_conn = sqlite3.connect()

# csv data path
data_path = '/Users/macbook/Development/database_crash_test/data/no_headers_brandenburger_gate_seriescalc.csv'

# create a duckdb readable table
csv = duckdb.read_csv(data_path)
print(type(csv))

# create a sql query to read the table
sql_query = "SELECT * FROM csv WHERE time > '20221231' & time < '20231231';'"

print(duckdb.sql(sql_query))
