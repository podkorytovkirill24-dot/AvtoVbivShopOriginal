def build_queue_csv(conn: sqlite3.Connection) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "phone", "status", "created_at", "assigned_at", "tariff_id"])
    rows = conn.execute(
        "SELECT id, phone, status, created_at, assigned_at, tariff_id "
        "FROM queue_numbers WHERE status IN ('queued','taken') ORDER BY created_at"
    ).fetchall()
    for r in rows:
        writer.writerow([r["id"], r["phone"], r["status"], r["created_at"], r["assigned_at"], r["tariff_id"]])
    return output.getvalue()
