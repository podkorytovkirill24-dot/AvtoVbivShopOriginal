def init_db() -> None:
    conn = get_conn()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            role TEXT DEFAULT 'user',
            is_approved INTEGER DEFAULT 1,
            is_blocked INTEGER DEFAULT 0,
            ref_code TEXT,
            referred_by INTEGER,
            subscription_until INTEGER DEFAULT 0,
            created_at INTEGER,
            last_seen INTEGER,
            iam_here_at INTEGER DEFAULT 0,
            iam_warned_at INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS tariffs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            price REAL DEFAULT 0,
            duration_min INTEGER DEFAULT 0,
            priority INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS offices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            chat_id INTEGER,
            thread_id INTEGER,
            is_active INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS departments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            office_id INTEGER,
            is_active INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS queue_numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reception_chat_id INTEGER,
            user_id INTEGER NOT NULL,
            username TEXT,
            phone TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            assigned_at INTEGER,
            completed_at INTEGER,
            worker_id INTEGER,
            worker_chat_id INTEGER,
            worker_msg_id INTEGER,
            worker_thread_id INTEGER,
            tariff_id INTEGER,
            department_id INTEGER,
            photo_file_id TEXT,
            qr_requested INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_queue_status
            ON queue_numbers (status, created_at);

        CREATE TABLE IF NOT EXISTS support_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            closed_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id INTEGER NOT NULL,
            sender_id INTEGER NOT NULL,
            text TEXT,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS access_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS reception_groups (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            tariff_id INTEGER,
            is_active INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS processing_topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            thread_id INTEGER NOT NULL,
            reception_chat_id INTEGER NOT NULL,
            chat_title TEXT,
            UNIQUE(chat_id, thread_id)
        );

        CREATE TABLE IF NOT EXISTS withdrawal_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS payouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            note TEXT,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_user_id INTEGER NOT NULL,
            admin_username TEXT,
            action TEXT NOT NULL,
            details TEXT,
            created_at INTEGER NOT NULL
        );
        """
    )
    migrate_db(conn)
    for key, value in DEFAULT_CONFIG.items():
        conn.execute(
            "INSERT INTO config (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO NOTHING",
            (key, value),
        )
    for admin_id in ENV_ADMIN_IDS:
        conn.execute(
            "INSERT INTO admins (user_id) VALUES (?) ON CONFLICT(user_id) DO NOTHING",
            (admin_id,),
        )
    conn.commit()
    conn.close()
