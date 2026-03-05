def fetch_next_queue(
    conn: sqlite3.Connection,
    dept_ids: List[int],
    reception_chat_id: Optional[int] = None,
) -> Optional[sqlite3.Row]:
    use_priorities = get_config_bool(conn, "use_priorities", True)
    params: List = []
    if reception_chat_id is not None:
        reception_filter = " AND q.reception_chat_id = ?"
        params.append(reception_chat_id)
    else:
        reception_filter = ""
    if dept_ids:
        dept_filter = " AND q.department_id IN ({})".format(",".join("?" for _ in dept_ids))
        params.extend(dept_ids)
    else:
        dept_filter = ""
    order = "t.priority DESC, q.created_at ASC" if use_priorities else "q.created_at ASC"
    query = (
        "SELECT q.*, t.name AS tariff_name, t.price, t.duration_min "
        "FROM queue_numbers q "
        "LEFT JOIN tariffs t ON q.tariff_id = t.id "
        "WHERE q.status = 'queued' "
        f"{reception_filter} "
        f"{dept_filter} "
        f"ORDER BY {order} LIMIT 1"
    )
    return conn.execute(query, tuple(params)).fetchone()
