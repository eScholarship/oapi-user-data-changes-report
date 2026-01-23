from pub_oapi_tools_common import ucpms_db

conn = ucpms_db.get_connection(env="prod")

with conn.cursor() as cursor:
    cursor.execute("EXEC UCOPreports.update_user_data_changes;")

conn.close()
