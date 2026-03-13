def migrate_db(conn: sqlite3.Connection) -> None:
    # Users
    _add_column(conn, "users", "role", "role TEXT DEFAULT 'user'")
    _add_column(conn, "users", "is_approved", "is_approved INTEGER DEFAULT 1")
    _add_column(conn, "users", "is_blocked", "is_blocked INTEGER DEFAULT 0")
    _add_column(conn, "users", "ref_code", "ref_code TEXT")
    _add_column(conn, "users", "referred_by", "referred_by INTEGER")
    _add_column(conn, "users", "subscription_until", "subscription_until INTEGER DEFAULT 0")
    _add_column(conn, "users", "created_at", "created_at INTEGER")
    _add_column(conn, "users", "last_seen", "last_seen INTEGER")
    _add_column(conn, "users", "iam_here_at", "iam_here_at INTEGER DEFAULT 0")
    _add_column(conn, "users", "iam_warned_at", "iam_warned_at INTEGER DEFAULT 0")

    # Tariffs
    _add_column(conn, "tariffs", "price", "price REAL DEFAULT 0")
    _add_column(conn, "tariffs", "duration_min", "duration_min INTEGER DEFAULT 0")
    _add_column(conn, "tariffs", "priority", "priority INTEGER DEFAULT 0")

    # Offices
    _add_column(conn, "offices", "chat_id", "chat_id INTEGER")
    _add_column(conn, "offices", "thread_id", "thread_id INTEGER")
    _add_column(conn, "offices", "is_active", "is_active INTEGER DEFAULT 1")

    # Departments
    _add_column(conn, "departments", "office_id", "office_id INTEGER")
    _add_column(conn, "departments", "is_active", "is_active INTEGER DEFAULT 1")

    # Queue numbers
    _add_column(conn, "queue_numbers", "reception_chat_id", "reception_chat_id INTEGER")
    _add_column(conn, "queue_numbers", "assigned_at", "assigned_at INTEGER")
    _add_column(conn, "queue_numbers", "completed_at", "completed_at INTEGER")
    _add_column(conn, "queue_numbers", "worker_id", "worker_id INTEGER")
    _add_column(conn, "queue_numbers", "worker_chat_id", "worker_chat_id INTEGER")
    _add_column(conn, "queue_numbers", "worker_msg_id", "worker_msg_id INTEGER")
    _add_column(conn, "queue_numbers", "worker_thread_id", "worker_thread_id INTEGER")
    _add_column(conn, "queue_numbers", "tariff_id", "tariff_id INTEGER")
    _add_column(conn, "queue_numbers", "department_id", "department_id INTEGER")
    _add_column(conn, "queue_numbers", "photo_file_id", "photo_file_id TEXT")
    _add_column(conn, "queue_numbers", "qr_requested", "qr_requested INTEGER DEFAULT 0")

    # Support
    _add_column(conn, "support_tickets", "closed_at", "closed_at INTEGER")
    _add_column(conn, "support_messages", "text", "text TEXT")

    # Access requests
    _add_column(conn, "access_requests", "status", "status TEXT NOT NULL DEFAULT 'pending'")
    _add_column(conn, "access_requests", "created_at", "created_at INTEGER")

    # Processing topics
    _add_column(conn, "processing_topics", "chat_title", "chat_title TEXT")

    # Withdrawals
    _add_column(conn, "withdrawal_requests", "updated_at", "updated_at INTEGER")
