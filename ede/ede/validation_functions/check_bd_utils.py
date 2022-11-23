

def ejecutar_sql(conn, sql_string: str, *args):
    data = conn.execute(sql_string, *args)
    if data is None or not data.returns_rows:
        data = []
    else:
        data = data.fetchall()
    return data