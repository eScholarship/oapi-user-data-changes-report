import boto3
import pyodbc

# Get AWS session
session = boto3.Session()


def main():
    creds = get_ssm_parameters(
        folder="/pub-oapi-tools/elements-reporting-db/prod",
        names=['driver', 'server', 'database', 'user', 'password'])

    # Connect to Elements Reporting DB
    conn = pyodbc.connect(
        driver=creds['driver'],
        server=(creds['server'] + ',1433'),
        database=creds['database'],
        uid=creds['user'],
        pwd=creds['password'],
        trustservercertificate='yes')

    conn.autocommit = True

    cursor = conn.cursor()
    cursor.execute(
        "EXEC UCOPreports.update_user_data_changes;")
    conn.commit()

    cursor.close()
    conn.close()


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
