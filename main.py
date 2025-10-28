from datetime import datetime
import subprocess
import pyodbc
import csv
import boto3
import requests

# Get AWS session
session = boto3.Session()


def main():
    today_string = datetime.today().strftime('%Y-%m-%d')

    # Get reporting DB creds
    reporting_db_creds = get_ssm_parameters(
        folder="/pub-oapi-tools/elements-reporting-db/prod",
        names=['driver', 'server', 'database', 'user', 'password'])

    # Get email addresses DB creds
    emails = get_ssm_parameters(
        folder="/pub-oapi-tools/emails",
        names=['devin', 'alainna'])

    # Load the sql file data from Github, replace a text string with today's date
    pgd_change_query = get_sql_from_github()
    pgd_change_query = pgd_change_query.replace('#REPLACE', today_string)

    pdg_change_report = query_reporting_db(reporting_db_creds, pgd_change_query)
    pdg_change_report_file = write_csv_file(pdg_change_report, "pdg_change_report", today_string)

    # Set up the mail process with attachment and email recipients
    subprocess_setup = ['mail',
                        '-s', 'TEST: PGD changes report',
                        '-a', pdg_change_report_file]
    subprocess_setup += [emails['devin'], emails['alainna']]

    # Text in the email body
    input_byte_string = b'''
        The attached CSV includes users' primary group changes in the span of one month.
        
        Future-Proofing: This automated email was sent from the following program:
        https://github.com/eScholarship/oapi-user-data-changes-report/ 
    '''

    # Run the subprocess with EOT input to send
    subprocess.run(
        subprocess_setup,
        input=input_byte_string,
        capture_output=True)


def query_reporting_db(creds, query):
    # Connect to Elements reporting db
    conn = pyodbc.connect(
        driver=creds['driver'],
        server=(creds['server'] + ',1433'),
        database=creds['database'],
        uid=creds['user'],
        pwd=creds['password'],
        trustservercertificate='yes')

    print(f"Connected to Elements reporting DB, querying.")
    conn.autocommit = True  # Required when queries use TRANSACTION
    cursor = conn.cursor()
    cursor.execute(query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows


def get_sql_from_github():
    github_raw_link = "https://raw.githubusercontent.com/eScholarship/oapi-user-data-changes-report/main/"
    input_file = "pgd_change_report.sql"
    response = requests.get(f"{github_raw_link}{input_file}")
    return response.text


def write_csv_file(data, filename, today_string):
    filename_with_date = f"output/{filename}-{today_string}.csv"
    with open(filename_with_date, "w") as outfile:
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    return filename_with_date


# Parses data from parameter store
def get_ssm_parameters(folder, names):
    print("Connect to SSM for parameters")
    ssm_client = session.client(service_name='ssm', region_name='us-west-2')

    param_names = [f"{folder}/{name}" for name in names]
    response = ssm_client.get_parameters(Names=param_names, WithDecryption=True)

    param_values = {
        (param['Name'].split('/')[-1]): param['Value']
        for param in response['Parameters']}

    return param_values


# Stub for main
if __name__ == '__main__':
    main()
