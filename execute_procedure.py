import pyodbc
from dotenv import dotenv_values
creds = dotenv_values()

# Connect to Elements Reporting DB
conn = pyodbc.connect(
    driver=creds['ELEMENTS_REPORTING_DB_DRIVER'],
    server=(creds['ELEMENTS_REPORTING_DB_SERVER'] + ',' + creds['ELEMENTS_REPORTING_DB_PORT']),
    database=creds['ELEMENTS_REPORTING_DB_DATABASE'],
    uid=creds['ELEMENTS_REPORTING_DB_USER'],
    pwd=creds['ELEMENTS_REPORTING_DB_PASSWORD'],
    trustservercertificate='yes')

conn.autocommit = True

cursor = conn.cursor()
cursor.execute("EXEC UCOPreports.update_user_data_changes;")
conn.commit()

cursor.close()
conn.close()

