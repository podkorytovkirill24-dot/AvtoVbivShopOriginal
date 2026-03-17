def _get_start_ts(row) -> int:
    return int(row["assigned_at"] or 0)


def _format_start_label(start_ts: int) -> str:
    return format_ts(start_ts) if start_ts else "- (РЅРµ РІР·СЏС‚)"


def build_report_detailed(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT q.phone, q.status, q.created_at, q.assigned_at, q.completed_at, q.user_id, u.username, "
        "t.name AS tariff, t.duration_min "
        "FROM queue_numbers q "
        "LEFT JOIN users u ON u.user_id = q.user_id "
        "LEFT JOIN tariffs t ON q.tariff_id = t.id "
        "WHERE q.completed_at IS NOT NULL AND q.status IN ('success','slip','error','canceled') "
        "ORDER BY q.completed_at DESC LIMIT 30"
    ).fetchall()
    lines = ["рџ“€ Р”РµС‚Р°Р»СЊРЅС‹Р№ РѕС‚С‡С‘С‚", "РџРµСЂРёРѕРґ: РїРѕСЃР»РµРґРЅРёРµ 30 Р·Р°РїРёСЃРµР№", ""]
    for r in rows:
        start_ts = _get_start_ts(r)
        if not start_ts:
            continue
        end_ts = int(r["completed_at"] or now_ts())
        duration_sec = max(0, end_ts - start_ts) if start_ts else 0
        duration_min = int(duration_sec // 60)
        duration_limit = r["duration_min"]
        if not start_ts:
            mark = "—"
        elif duration_limit is None or int(duration_limit or 0) <= 0:
            mark = "-"
        else:
            limit_sec = int(duration_limit or 0) * 60
            mark = "вњ…" if duration_sec >= limit_sec else "вќЊ"
        start_label = _format_start_label(start_ts)
        tariff = r["tariff"] or "-"
        lines.append(
            f"вЂў {r['phone']} | {format_user_label(r['user_id'], r['username'])} | "
            f"{status_human(r['status'])} | {tariff} | "
            f"РІСЃС‚Р°Р»: {start_label} | СЃР»РµС‚РµР»: {format_ts(r['completed_at'])} | "
            f"СЃС‚РѕСЏР»: {format_duration(duration_sec)} ({duration_min} РјРёРЅ) {mark}"
        )
    return "\n".join(lines)


def _iter_report_rows(conn: sqlite3.Connection, limit: int = 50):
    return conn.execute(
        "SELECT q.phone, q.status, q.created_at, q.assigned_at, q.completed_at, q.user_id, u.username, "
        "t.name AS tariff, t.duration_min "
        "FROM queue_numbers q "
        "LEFT JOIN users u ON u.user_id = q.user_id "
        "LEFT JOIN tariffs t ON q.tariff_id = t.id "
        "WHERE q.completed_at IS NOT NULL "
        "ORDER BY q.completed_at DESC LIMIT ?",
        (limit,),
    ).fetchall()


def build_report_stood(conn: sqlite3.Connection) -> str:
    rows = _iter_report_rows(conn, limit=50)
    lines = ["вњ… РќРѕРјРµСЂР° РѕС‚СЃС‚РѕСЏРІС€РёРµ", "РџРµСЂРёРѕРґ: РїРѕСЃР»РµРґРЅРёРµ 50 Р·Р°РІРµСЂС€С‘РЅРЅС‹С…", ""]
    shown = 0
    for r in rows:
        duration_limit = r["duration_min"]
        if duration_limit is None:
            continue
        limit_sec = int(duration_limit or 0) * 60
        if limit_sec <= 0:
            continue
        start_ts = _get_start_ts(r)
        if not start_ts:
            continue
        end_ts = int(r["completed_at"] or now_ts())
        duration_sec = max(0, end_ts - start_ts)
        if duration_sec < limit_sec:
            continue
        duration_min = int(duration_sec // 60)
        shown += 1
        start_label = format_ts(start_ts)
        tariff = r["tariff"] or "-"
        lines.append(
            f"вЂў {r['phone']} | {format_user_label(r['user_id'], r['username'])} | "
            f"{status_human(r['status'])} | {tariff} | "
            f"РІСЃС‚Р°Р»: {start_label} | СЃР»РµС‚РµР»: {format_ts(r['completed_at'])} | "
            f"СЃС‚РѕСЏР»: {format_duration(duration_sec)} ({duration_min} РјРёРЅ)"
        )
    if shown == 0:
        lines.append("РќРµС‚ Р·Р°РїРёСЃРµР№.")
    return "\n".join(lines)


def build_report_not_stood(conn: sqlite3.Connection) -> str:
    rows = _iter_report_rows(conn, limit=50)
    lines = ["вќЊ РќРѕРјРµСЂР° РЅРµ РѕС‚СЃС‚РѕСЏРІС€РёРµ", "РџРµСЂРёРѕРґ: РїРѕСЃР»РµРґРЅРёРµ 50 Р·Р°РІРµСЂС€С‘РЅРЅС‹С…", ""]
    shown = 0
    for r in rows:
        duration_limit = r["duration_min"]
        if duration_limit is None:
            continue
        limit_sec = int(duration_limit or 0) * 60
        if limit_sec <= 0:
            continue
        start_ts = _get_start_ts(r)
        end_ts = int(r["completed_at"] or now_ts())
        duration_sec = max(0, end_ts - start_ts) if start_ts else 0
        if start_ts and duration_sec >= limit_sec:
            continue
        duration_min = int(duration_sec // 60)
        shown += 1
        start_label = _format_start_label(start_ts)
        tariff = r["tariff"] or "-"
        lines.append(
            f"вЂў {r['phone']} | {format_user_label(r['user_id'], r['username'])} | "
            f"{status_human(r['status'])} | {tariff} | "
            f"РІСЃС‚Р°Р»: {start_label} | СЃР»РµС‚РµР»: {format_ts(r['completed_at'])} | "
            f"СЃС‚РѕСЏР»: {format_duration(duration_sec)} ({duration_min} РјРёРЅ)"
        )
    if shown == 0:
        lines.append("РќРµС‚ Р·Р°РїРёСЃРµР№.")
    return "\n".join(lines)

