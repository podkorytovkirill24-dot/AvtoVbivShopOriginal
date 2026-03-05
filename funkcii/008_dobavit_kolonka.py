def _add_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if _column_exists(conn, table, column):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
