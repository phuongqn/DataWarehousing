import mysql.connector
import csv
import sys

def get_table_names(cursor):
    cursor.execute('show tables')
    table_names = cursor.fetchall()
    tables = [x[0] for x in table_names]
    return tables

def to_csv(tables, cursor):
    for table in tables:
        header = []
        cursor.execute("select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '" + table + "'")
        for c in cursor:
            header.append(str(c)[2: -3])

        path = table + '.csv'
        with open(path, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            cursor.execute('SELECT * FROM ' + table)
            for c in cursor:
                writer.writerow(c)
            
def export_csv (db):
    cnx = mysql.connector.connect(
        host = 'localhost',
        user = 'inf551',
        password = 'inf551',
        database=db)
    cur = cnx.cursor()
    tables= get_table_names(cur)
    to_csv(tables, cur)

if __name__ == "__main__":
    DB = sys.argv[1]
    export_csv(DB)
    
