from pub_oapi_tools_common import aws_lambda
from pub_oapi_tools_common import ucpms_db

from datetime import datetime
import subprocess
import csv
import requests


def main():
    today_string = datetime.today().strftime('%Y-%m-%d')

    # Load the sql file data from Github, replace a text string with today's date
    pgd_change_query = get_sql_from_github()
    pgd_change_query = pgd_change_query.replace('#REPLACE', today_string)

    pdg_change_report = query_reporting_db(pgd_change_query)
    pdg_change_report_file = write_csv_file(pdg_change_report, "pdg_change_report", today_string)

    # Get email addresses DB creds
    param_req = {"emails": {
        "folder": "pub-oapi-tools/emails",
        "names": ['devin', 'alainna']}}
    param_return = aws_lambda.get_parameters(param_req)
    emails = param_return['emails'].values()

    # Set up the mail process with attachment and email recipients
    subprocess_setup = ['mail',
                        '-s', 'PGD changes report',
                        '-a', pdg_change_report_file]
    subprocess_setup += emails

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


def query_reporting_db(query):
    conn = ucpms_db.get_connection(env="prod")
    cursor = conn.cursor()
    cursor.execute(query)
    rows = ucpms_db.get_dict_list(cursor)
    conn.close()
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


# Stub for main
if __name__ == '__main__':
    main()
