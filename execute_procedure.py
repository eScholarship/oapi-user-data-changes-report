from pub_oapi_tools_common import ucpms_db


def main():
    conn = ucpms_db.get_connection(env="prod", autocommit=True)
    cursor = conn.cursor()
    cursor.execute("EXEC UCOPreports.update_user_data_changes;")
    conn.commit()
    cursor.close()
    conn.close()


# Stub for main
if __name__ == '__main__':
    main()
