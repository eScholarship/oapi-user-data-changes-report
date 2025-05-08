from datetime import datetime
import subprocess
from dotenv import dotenv_values
import pyodbc
import csv


def main():
    creds = dotenv_values()
    today_string = datetime.today().strftime('%Y-%m-%d')

    with open('pdg_change_report.sql') as f:
        pgd_change_query = f.read().replace('#REPLACE', today_string)

    pdg_change_report = query_reporting_db(creds, pgd_change_query)
    pdg_change_report_file = write_csv_file(pdg_change_report, "pdg_change_report", today_string)

    # Set up the mail process with attachment and email recipients
    subprocess_setup = ['mail',
                        '-s', 'TEST: PGD changes report',
                        '-a', pdg_change_report_file]
    subprocess_setup += [creds['DEVIN'], creds['ALAINNA']]

    # Text in the email body
    input_byte_string = b'''
        The attached CSV includes users' primary group changes
        in the span of one month (for the tests, it's from 2025-05-01)
    '''

    # Run the subprocess with EOT input to send
    subprocess.run(
        subprocess_setup,
        input=input_byte_string,
        capture_output=True)


def query_reporting_db(creds, query):
    # Connect to Elements reporting db
    conn = pyodbc.connect(
        driver=creds['ELEMENTS_REPORTING_DB_DRIVER'],
        server=(creds['ELEMENTS_REPORTING_DB_SERVER'] + ',' + creds['ELEMENTS_REPORTING_DB_PORT']),
        database=creds['ELEMENTS_REPORTING_DB_DATABASE'],
        uid=creds['ELEMENTS_REPORTING_DB_USER'],
        pwd=creds['ELEMENTS_REPORTING_DB_PASSWORD'],
        trustservercertificate='yes')

    print(f"Connected to Elements reporting DB, querying: {input_file}")
    conn.autocommit = True  # Required when queries use TRANSACTION
    cursor = conn.cursor()
    cursor.execute(query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows


def write_csv_file(data, filename, today_string):
    filename_with_date = f"output/{filename}-{today_string}.csv"
    with open(filename_with_date, "w") as outfile:
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    return filename_with_date


# Stub for main
if __name__ == '__main__':
    main()
