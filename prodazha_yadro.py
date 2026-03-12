# -*- coding: utf-8 -*-
import json
import os
import sqlite3
import subprocess
import threading
import time
import base64
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from cryptography.fernet import Fernet
except Exception:
    Fernet = None


SECONDS_IN_DAY = 86400


def now_ts() -> int:
    return int(time.time())


def load_env(path: str = ".env") -> None:
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("\"'").strip()
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        return


def parse_id_set(raw: str) -> set:
    result = set()
    for part in (raw or "").split(","):
        item = part.strip()
        if not item:
            continue
        try:
            result.add(int(item))
        except ValueError:
            continue
    return result


@dataclass
class Plan:
    key: str
    title: str
    duration_days: int
    price_usdt: float


@dataclass
class SalesConfig:
    sales_bot_token: str
    crypto_pay_token: str
    crypto_api_base: str
    crypto_asset: str
    sales_db_path: Path
    instances_dir: Path
    project_root: Path
    project_main_path: Path
    python_executable: str
    check_interval_seconds: int
    owner_ids: set
    extra_admin_ids: set
    plans: Dict[str, Plan]
    secret_key_file: Path

    @staticmethod
    def from_env(project_root: Path) -> "SalesConfig":
        load_env(str(project_root / ".env"))

        sales_bot_token = os.getenv("SALES_BOT_TOKEN", "").strip()
        if not sales_bot_token:
            raise RuntimeError("SALES_BOT_TOKEN ne ukazan v .env")

        crypto_pay_token = os.getenv("CRYPTO_PAY_TOKEN", "").strip()
        if not crypto_pay_token:
            raise RuntimeError("CRYPTO_PAY_TOKEN ne ukazan v .env")

        db_path = Path(os.getenv("SALES_DB_PATH", str(project_root / "sales.db"))).expanduser()
        if not db_path.is_absolute():
            db_path = (project_root / db_path).resolve()

        instances_dir = Path(os.getenv("SALES_INSTANCES_DIR", str(project_root / "instances"))).expanduser()
        if not instances_dir.is_absolute():
            instances_dir = (project_root / instances_dir).resolve()

        main_path = Path(os.getenv("CLIENT_BOT_MAIN_PATH", str(project_root / "main.py"))).expanduser()
        if not main_path.is_absolute():
            main_path = (project_root / main_path).resolve()

        python_executable = os.getenv("CLIENT_BOT_PYTHON", "").strip() or os.sys.executable
        check_interval = int(os.getenv("SALES_CHECK_INTERVAL_SECONDS", "20"))
        crypto_api_base = os.getenv("CRYPTO_API_BASE", "https://pay.crypt.bot/api").strip().rstrip("/")
        crypto_asset = os.getenv("CRYPTO_ASSET", "USDT").strip().upper() or "USDT"

        owner_ids = parse_id_set(os.getenv("SALES_OWNER_IDS", ""))
        extra_admin_ids = parse_id_set(os.getenv("CLIENT_EXTRA_ADMIN_IDS", ""))

        plan_week = Plan(
            key="week",
            title="Неделя",
            duration_days=7,
            price_usdt=float(os.getenv("SELL_PRICE_WEEK", "10")),
        )
        plan_month = Plan(
            key="month",
            title="Месяц",
            duration_days=30,
            price_usdt=float(os.getenv("SELL_PRICE_MONTH", "30")),
        )
        plan_lifetime = Plan(
            key="lifetime",
            title="Навсегда",
            duration_days=0,
            price_usdt=float(os.getenv("SELL_PRICE_LIFETIME", "99")),
        )
        plans = {p.key: p for p in (plan_week, plan_month, plan_lifetime)}

        secret_key_file = Path(os.getenv("SALES_SECRET_FILE", str(project_root / ".sales_secret.key"))).expanduser()
        if not secret_key_file.is_absolute():
            secret_key_file = (project_root / secret_key_file).resolve()

        return SalesConfig(
            sales_bot_token=sales_bot_token,
            crypto_pay_token=crypto_pay_token,
            crypto_api_base=crypto_api_base,
            crypto_asset=crypto_asset,
            sales_db_path=db_path,
            instances_dir=instances_dir,
            project_root=project_root,
            project_main_path=main_path,
            python_executable=python_executable,
            check_interval_seconds=max(10, check_interval),
            owner_ids=owner_ids,
            extra_admin_ids=extra_admin_ids,
            plans=plans,
            secret_key_file=secret_key_file,
        )


def ensure_dirs(cfg: SalesConfig) -> None:
    cfg.instances_dir.mkdir(parents=True, exist_ok=True)
    cfg.sales_db_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.secret_key_file.parent.mkdir(parents=True, exist_ok=True)


def get_conn(cfg: SalesConfig) -> sqlite3.Connection:
    conn = sqlite3.connect(str(cfg.sales_db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(cfg: SalesConfig) -> None:
    conn = get_conn(cfg)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS plans (
            plan_key TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            duration_days INTEGER NOT NULL,
            price_usdt REAL NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            plan_key TEXT NOT NULL,
            amount_usdt REAL NOT NULL,
            invoice_id INTEGER NOT NULL UNIQUE,
            invoice_url TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            paid_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS licenses (
            user_id INTEGER PRIMARY KEY,
            plan_key TEXT NOT NULL,
            token_encrypted TEXT,
            bot_id INTEGER,
            bot_username TEXT,
            status TEXT NOT NULL,
            expires_at INTEGER,
            instance_dir TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            last_started_at INTEGER,
            last_stopped_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS priority_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            granted_by INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()

    sync_plan_prices(cfg)


def sync_plan_prices(cfg: SalesConfig) -> None:
    conn = get_conn(cfg)
    ts = now_ts()
    for plan in cfg.plans.values():
        conn.execute(
            "INSERT INTO plans(plan_key, title, duration_days, price_usdt, updated_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(plan_key) DO UPDATE SET "
            "title=excluded.title, duration_days=excluded.duration_days, "
            "price_usdt=excluded.price_usdt, updated_at=excluded.updated_at",
            (plan.key, plan.title, plan.duration_days, plan.price_usdt, ts),
        )
    conn.commit()
    conn.close()


def upsert_user(cfg: SalesConfig, tg_user) -> None:
    ts = now_ts()
    conn = get_conn(cfg)
    conn.execute(
        "INSERT INTO users(user_id, username, first_name, last_name, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "username=excluded.username, first_name=excluded.first_name, "
        "last_name=excluded.last_name, updated_at=excluded.updated_at",
        (
            int(tg_user.id),
            tg_user.username,
            tg_user.first_name,
            tg_user.last_name,
            ts,
            ts,
        ),
    )
    conn.commit()
    conn.close()


def resolve_user_by_username_or_id(cfg: SalesConfig, raw: str) -> Optional[sqlite3.Row]:
    value = (raw or "").strip()
    if not value:
        return None

    conn = get_conn(cfg)
    try:
        if value.lstrip("-").isdigit():
            return conn.execute("SELECT * FROM users WHERE user_id = ?", (int(value),)).fetchone()

        username = value.lstrip("@").strip().lower()
        if not username:
            return None
        return conn.execute(
            "SELECT * FROM users WHERE lower(username) = ? ORDER BY updated_at DESC LIMIT 1",
            (username,),
        ).fetchone()
    finally:
        conn.close()


def is_priority_user(cfg: SalesConfig, user_id: int) -> bool:
    conn = get_conn(cfg)
    row = conn.execute("SELECT 1 FROM priority_users WHERE user_id = ?", (int(user_id),)).fetchone()
    conn.close()
    return row is not None


def grant_priority(cfg: SalesConfig, user_id: int, username: str, granted_by: int) -> None:
    ts = now_ts()
    conn = get_conn(cfg)
    conn.execute(
        "INSERT INTO priority_users(user_id, username, granted_by, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "username=excluded.username, granted_by=excluded.granted_by, updated_at=excluded.updated_at",
        (int(user_id), (username or "").strip().lstrip("@"), int(granted_by), ts, ts),
    )
    conn.commit()
    conn.close()


def revoke_priority(cfg: SalesConfig, user_id: int) -> bool:
    conn = get_conn(cfg)
    cur = conn.execute("DELETE FROM priority_users WHERE user_id = ?", (int(user_id),))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def list_priority_users(cfg: SalesConfig) -> List[sqlite3.Row]:
    conn = get_conn(cfg)
    rows = conn.execute(
        "SELECT p.user_id, p.username, p.granted_by, p.created_at, p.updated_at, u.first_name, u.last_name "
        "FROM priority_users p "
        "LEFT JOIN users u ON u.user_id = p.user_id "
        "ORDER BY p.updated_at DESC"
    ).fetchall()
    conn.close()
    return rows


def get_admin_stats(cfg: SalesConfig) -> Dict[str, float]:
    conn = get_conn(cfg)
    stats: Dict[str, float] = {}

    stats["users_total"] = float(conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] or 0)
    stats["orders_total"] = float(conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] or 0)
    stats["orders_paid"] = float(conn.execute("SELECT COUNT(*) FROM orders WHERE status='paid'").fetchone()[0] or 0)
    stats["orders_pending"] = float(conn.execute("SELECT COUNT(*) FROM orders WHERE status='pending'").fetchone()[0] or 0)
    stats["orders_expired"] = float(conn.execute("SELECT COUNT(*) FROM orders WHERE status='expired'").fetchone()[0] or 0)

    paid_sum = conn.execute(
        "SELECT COALESCE(SUM(amount_usdt), 0) FROM orders WHERE status='paid'"
    ).fetchone()[0]
    stats["revenue_paid_usdt"] = float(paid_sum or 0.0)

    stats["licenses_total"] = float(conn.execute("SELECT COUNT(*) FROM licenses").fetchone()[0] or 0)
    stats["licenses_active"] = float(conn.execute("SELECT COUNT(*) FROM licenses WHERE status='active'").fetchone()[0] or 0)
    stats["licenses_pending_token"] = float(
        conn.execute("SELECT COUNT(*) FROM licenses WHERE status='pending_token'").fetchone()[0] or 0
    )
    stats["licenses_expired"] = float(conn.execute("SELECT COUNT(*) FROM licenses WHERE status='expired'").fetchone()[0] or 0)
    stats["licenses_stopped"] = float(conn.execute("SELECT COUNT(*) FROM licenses WHERE status='stopped'").fetchone()[0] or 0)

    stats["priority_users"] = float(conn.execute("SELECT COUNT(*) FROM priority_users").fetchone()[0] or 0)
    conn.close()
    return stats


def ensure_secret_key(cfg: SalesConfig) -> bytes:
    raw_env = os.getenv("SALES_SECRET_KEY", "").strip()
    if raw_env:
        return raw_env.encode("utf-8")

    if cfg.secret_key_file.exists():
        saved = cfg.secret_key_file.read_text(encoding="utf-8").strip()
        if saved:
            return saved.encode("utf-8")

    if Fernet is not None:
        key = Fernet.generate_key()
    else:
        key = base64.urlsafe_b64encode(os.urandom(32))
    cfg.secret_key_file.write_text(key.decode("utf-8"), encoding="utf-8")
    return key


class TokenCipher:
    def __init__(self, key: bytes):
        self._fernet = Fernet(key) if Fernet is not None else None
        self._raw_key = hashlib.sha256(key).digest()

    def _xor_transform(self, text: str) -> str:
        data = text.encode("utf-8")
        out = bytearray()
        for i, b in enumerate(data):
            out.append(b ^ self._raw_key[i % len(self._raw_key)])
        return base64.urlsafe_b64encode(bytes(out)).decode("utf-8")

    def _xor_restore(self, text: str) -> str:
        data = base64.urlsafe_b64decode(text.encode("utf-8"))
        out = bytearray()
        for i, b in enumerate(data):
            out.append(b ^ self._raw_key[i % len(self._raw_key)])
        return bytes(out).decode("utf-8")

    def encrypt(self, token: str) -> str:
        if self._fernet is not None:
            value = self._fernet.encrypt(token.encode("utf-8")).decode("utf-8")
            return f"f:{value}"
        return f"x:{self._xor_transform(token)}"

    def decrypt(self, token_encrypted: str) -> str:
        if token_encrypted.startswith("f:"):
            raw = token_encrypted[2:]
            if self._fernet is None:
                raise RuntimeError("Token zashifrovan Fernet, no cryptography ne ustanovlen.")
            return self._fernet.decrypt(raw.encode("utf-8")).decode("utf-8")
        if token_encrypted.startswith("x:"):
            return self._xor_restore(token_encrypted[2:])

        if self._fernet is not None:
            return self._fernet.decrypt(token_encrypted.encode("utf-8")).decode("utf-8")
        return self._xor_restore(token_encrypted)


class CryptoPayClient:
    def __init__(self, api_token: str, base_url: str, asset: str):
        self.api_token = api_token
        self.base_url = base_url.rstrip("/")
        self.asset = asset

    def _request(self, method: str, endpoint: str, body: Optional[dict] = None, query: Optional[dict] = None) -> dict:
        query_string = ""
        if query:
            query_string = "?" + urlencode(query)
        url = f"{self.base_url}/{endpoint.lstrip('/')}{query_string}"

        headers = {
            "Crypto-Pay-API-Token": self.api_token,
            "Content-Type": "application/json",
            # Helps avoid Cloudflare 1010 false denies on some networks.
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        payload = None
        if body is not None:
            payload = json.dumps(body).encode("utf-8")

        req = Request(url=url, method=method.upper(), headers=headers, data=payload)
        try:
            with urlopen(req, timeout=25) as resp:
                raw = resp.read().decode("utf-8")
        except HTTPError as exc:
            body_text = ""
            try:
                body_text = exc.read().decode("utf-8")
            except Exception:
                body_text = ""
            if exc.code == 403:
                hint = (
                    "403 Forbidden: Crypto Pay не принимает токен.\n"
                    "Проверьте, что CRYPTO_PAY_TOKEN взят из Crypto Pay (а не из BotFather), "
                    "и что приложение активно."
                )
                if body_text:
                    hint += f"\nОтвет API: {body_text}"
                    if "1010" in body_text:
                        hint += (
                            "\nПодсказка: это может быть Cloudflare блокировка или "
                            "несовпадение testnet/mainnet."
                        )
                raise RuntimeError(hint) from exc
            message = f"HTTP {exc.code} при обращении к Crypto Pay."
            if body_text:
                message += f" Ответ API: {body_text}"
            raise RuntimeError(message) from exc
        except URLError as exc:
            raise RuntimeError(f"Сетевая ошибка Crypto Pay: {exc}") from exc
        data = json.loads(raw)
        if not data.get("ok"):
            raise RuntimeError(f"Crypto API error: {raw}")
        return data.get("result")

    def create_invoice(self, amount_usdt: float, description: str, payload: str = "") -> dict:
        body = {
            "asset": self.asset,
            "amount": f"{amount_usdt:.2f}",
            "description": description[:1024],
        }
        if payload:
            body["payload"] = payload[:128]
        return self._request("POST", "createInvoice", body=body)

    def get_invoice(self, invoice_id: int) -> Optional[dict]:
        result = self._request("GET", "getInvoices", query={"invoice_ids": str(invoice_id)})
        if isinstance(result, list):
            return result[0] if result else None
        if isinstance(result, dict):
            items = result.get("items")
            if isinstance(items, list) and items:
                return items[0]
        return None


def create_order(cfg: SalesConfig, user_id: int, plan_key: str, amount_usdt: float, invoice_id: int, invoice_url: str) -> int:
    conn = get_conn(cfg)
    ts = now_ts()
    cur = conn.execute(
        "INSERT INTO orders(user_id, plan_key, amount_usdt, invoice_id, invoice_url, status, created_at) "
        "VALUES (?, ?, ?, ?, ?, 'pending', ?)",
        (user_id, plan_key, amount_usdt, invoice_id, invoice_url, ts),
    )
    conn.commit()
    order_id = int(cur.lastrowid)
    conn.close()
    return order_id


def get_order(cfg: SalesConfig, order_id: int) -> Optional[sqlite3.Row]:
    conn = get_conn(cfg)
    row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    conn.close()
    return row


def get_latest_pending_order(cfg: SalesConfig, user_id: int) -> Optional[sqlite3.Row]:
    conn = get_conn(cfg)
    row = conn.execute(
        "SELECT * FROM orders WHERE user_id = ? AND status = 'pending' ORDER BY id DESC LIMIT 1",
        (user_id,),
    ).fetchone()
    conn.close()
    return row


def mark_order_status(cfg: SalesConfig, order_id: int, status: str) -> None:
    conn = get_conn(cfg)
    if status == "paid":
        conn.execute(
            "UPDATE orders SET status = ?, paid_at = ? WHERE id = ?",
            (status, now_ts(), order_id),
        )
    else:
        conn.execute("UPDATE orders SET status = ? WHERE id = ?", (status, order_id))
    conn.commit()
    conn.close()


def get_license(cfg: SalesConfig, user_id: int) -> Optional[sqlite3.Row]:
    conn = get_conn(cfg)
    row = conn.execute("SELECT * FROM licenses WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return row


def _resolve_instance_dir(cfg: SalesConfig, user_id: int) -> Path:
    return cfg.instances_dir / f"user_{user_id}"


def apply_paid_plan(cfg: SalesConfig, user_id: int, plan_key: str) -> sqlite3.Row:
    plan = cfg.plans.get(plan_key)
    if not plan:
        raise RuntimeError(f"Unknown plan: {plan_key}")

    ts = now_ts()
    conn = get_conn(cfg)
    row = conn.execute("SELECT * FROM licenses WHERE user_id = ?", (user_id,)).fetchone()

    if row is None:
        expires_at = None if plan.duration_days == 0 else ts + plan.duration_days * SECONDS_IN_DAY
        instance_dir = str(_resolve_instance_dir(cfg, user_id))
        status = "pending_token"
        conn.execute(
            "INSERT INTO licenses(user_id, plan_key, token_encrypted, bot_id, bot_username, status, expires_at, instance_dir, created_at, updated_at) "
            "VALUES (?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?)",
            (user_id, plan.key, status, expires_at, instance_dir, ts, ts),
        )
    else:
        old_expires = row["expires_at"]
        token_present = bool(row["token_encrypted"])

        if old_expires is None:
            new_expires = None
        elif plan.duration_days == 0:
            new_expires = None
        else:
            base = ts
            if old_expires and old_expires > ts:
                base = old_expires
            new_expires = base + plan.duration_days * SECONDS_IN_DAY

        status = "active" if token_present else "pending_token"
        conn.execute(
            "UPDATE licenses SET plan_key = ?, status = ?, expires_at = ?, updated_at = ? WHERE user_id = ?",
            (plan.key, status, new_expires, ts, user_id),
        )

    conn.commit()
    updated = conn.execute("SELECT * FROM licenses WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return updated


def attach_token_to_license(
    cfg: SalesConfig,
    cipher: TokenCipher,
    user_id: int,
    token_plain: str,
    bot_id: int,
    bot_username: str,
) -> sqlite3.Row:
    encrypted = cipher.encrypt(token_plain)
    ts = now_ts()
    conn = get_conn(cfg)
    row = conn.execute("SELECT * FROM licenses WHERE user_id = ?", (user_id,)).fetchone()
    if row is None:
        raise RuntimeError("Snachala nuzhno oplatit tarif.")

    expires_at = row["expires_at"]
    if expires_at is not None and expires_at <= ts:
        status = "expired"
    else:
        status = "active"

    conn.execute(
        "UPDATE licenses SET token_encrypted = ?, bot_id = ?, bot_username = ?, status = ?, updated_at = ? WHERE user_id = ?",
        (encrypted, bot_id, bot_username, status, ts, user_id),
    )
    conn.commit()
    updated = conn.execute("SELECT * FROM licenses WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return updated


def is_license_expired(row: sqlite3.Row, at_ts: Optional[int] = None) -> bool:
    ts = at_ts or now_ts()
    expires_at = row["expires_at"]
    return expires_at is not None and expires_at <= ts


def normalize_license_status(cfg: SalesConfig, user_id: int) -> Optional[sqlite3.Row]:
    conn = get_conn(cfg)
    row = conn.execute("SELECT * FROM licenses WHERE user_id = ?", (user_id,)).fetchone()
    if row is None:
        conn.close()
        return None

    ts = now_ts()
    status = row["status"]
    token_present = bool(row["token_encrypted"])
    expired = is_license_expired(row, ts)

    new_status = status
    if expired:
        new_status = "expired"
    elif token_present:
        if status in ("expired", "pending_token", "stopped"):
            new_status = "active"
    else:
        new_status = "pending_token"

    if new_status != status:
        conn.execute("UPDATE licenses SET status = ?, updated_at = ? WHERE user_id = ?", (new_status, ts, user_id))
        conn.commit()

    row = conn.execute("SELECT * FROM licenses WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return row


def list_licenses(cfg: SalesConfig) -> List[sqlite3.Row]:
    conn = get_conn(cfg)
    rows = conn.execute("SELECT * FROM licenses ORDER BY updated_at DESC").fetchall()
    conn.close()
    return rows


def set_license_status(cfg: SalesConfig, user_id: int, status: str) -> None:
    conn = get_conn(cfg)
    conn.execute(
        "UPDATE licenses SET status = ?, updated_at = ?, last_stopped_at = ? WHERE user_id = ?",
        (status, now_ts(), now_ts(), user_id),
    )
    conn.commit()
    conn.close()


def record_license_started(cfg: SalesConfig, user_id: int) -> None:
    conn = get_conn(cfg)
    conn.execute(
        "UPDATE licenses SET status = 'active', updated_at = ?, last_started_at = ? WHERE user_id = ?",
        (now_ts(), now_ts(), user_id),
    )
    conn.commit()
    conn.close()


@dataclass
class RunningProcess:
    process: subprocess.Popen
    stdout_handle: object
    stderr_handle: object


class InstanceSupervisor:
    def __init__(self, cfg: SalesConfig, cipher: TokenCipher):
        self.cfg = cfg
        self.cipher = cipher
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._running: Dict[int, RunningProcess] = {}

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="sales-supervisor")
        self._thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        with self._lock:
            user_ids = list(self._running.keys())
        for user_id in user_ids:
            self._stop_process(user_id)

    def is_running(self, user_id: int) -> bool:
        with self._lock:
            item = self._running.get(user_id)
            if not item:
                return False
            if item.process.poll() is not None:
                self._cleanup_user(user_id)
                return False
            return True

    def sync_user(self, user_id: int) -> None:
        row = normalize_license_status(self.cfg, user_id)
        if row is None:
            self._stop_process(user_id)
            return
        self._apply_row(row)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                rows = list_licenses(self.cfg)
                for row in rows:
                    self._apply_row(row)
            except Exception:
                pass
            self._stop_event.wait(self.cfg.check_interval_seconds)

    def _apply_row(self, row: sqlite3.Row) -> None:
        user_id = int(row["user_id"])
        normalized = normalize_license_status(self.cfg, user_id)
        if normalized is None:
            self._stop_process(user_id)
            return

        should_run = normalized["status"] == "active" and bool(normalized["token_encrypted"])
        if should_run:
            self._ensure_running(normalized)
        else:
            self._stop_process(user_id)

    def _build_admin_ids(self, user_id: int) -> str:
        admin_ids = {int(user_id)}
        admin_ids.update(self.cfg.extra_admin_ids)
        return ",".join(str(item) for item in sorted(admin_ids))

    def _ensure_running(self, row: sqlite3.Row) -> None:
        user_id = int(row["user_id"])
        if self.is_running(user_id):
            return

        token = self.cipher.decrypt(row["token_encrypted"])
        instance_dir = Path(row["instance_dir"])
        instance_dir.mkdir(parents=True, exist_ok=True)

        out_path = instance_dir / "bot_stdout.log"
        err_path = instance_dir / "bot_stderr.log"
        stdout_handle = open(out_path, "a", encoding="utf-8")
        stderr_handle = open(err_path, "a", encoding="utf-8")

        if token == self.cfg.sales_bot_token:
            try:
                stderr_handle.write(
                    "ERROR: token for sales bot was provided; client bot will not be started.\n"
                )
                stderr_handle.flush()
            finally:
                try:
                    stdout_handle.close()
                except Exception:
                    pass
                try:
                    stderr_handle.close()
                except Exception:
                    pass
            return

        env = os.environ.copy()
        env["BOT_TOKEN"] = token
        env["BOT_DB_PATH"] = str(instance_dir / "bot.db")
        env["ADMIN_IDS"] = self._build_admin_ids(user_id)
        env["MINI_APP_PORT"] = "0"
        env["MINI_APP_BASE_URL"] = ""
        env["BOT_USERNAME"] = (row["bot_username"] or "").strip().lstrip("@")

        try:
            process = subprocess.Popen(
                [self.cfg.python_executable, str(self.cfg.project_main_path)],
                cwd=str(self.cfg.project_root),
                env=env,
                stdout=stdout_handle,
                stderr=stderr_handle,
            )
        except Exception as exc:
            try:
                stderr_handle.write(f"ERROR: failed to start bot process: {exc}\n")
                stderr_handle.flush()
            finally:
                try:
                    stdout_handle.close()
                except Exception:
                    pass
                try:
                    stderr_handle.close()
                except Exception:
                    pass
            return

        time.sleep(0.2)
        if process.poll() is not None:
            try:
                stderr_handle.write(
                    f"ERROR: bot process exited immediately with code {process.returncode}\n"
                )
                stderr_handle.flush()
            finally:
                try:
                    stdout_handle.close()
                except Exception:
                    pass
                try:
                    stderr_handle.close()
                except Exception:
                    pass
            return
        with self._lock:
            self._running[user_id] = RunningProcess(
                process=process,
                stdout_handle=stdout_handle,
                stderr_handle=stderr_handle,
            )
        record_license_started(self.cfg, user_id)

    def _cleanup_user(self, user_id: int) -> None:
        with self._lock:
            item = self._running.pop(user_id, None)
        if not item:
            return
        try:
            item.stdout_handle.close()
        except Exception:
            pass
        try:
            item.stderr_handle.close()
        except Exception:
            pass

    def _stop_process(self, user_id: int) -> None:
        with self._lock:
            item = self._running.get(user_id)
        if not item:
            return

        proc = item.process
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=8)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        self._cleanup_user(user_id)


def verify_bot_token(token: str) -> Tuple[bool, Optional[dict], str]:
    token = (token or "").strip()
    if ":" not in token:
        return False, None, "Токен не похож на Telegram токен."

    url = f"https://api.telegram.org/bot{token}/getMe"
    req = Request(url=url, method="GET")
    try:
        with urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
        data = json.loads(raw)
    except Exception as exc:
        return False, None, f"Не удалось проверить токен: {exc}"

    if not data.get("ok"):
        return False, None, "Telegram не принял этот токен."
    result = data.get("result") or {}
    return True, result, ""


def format_expiration(expires_at: Optional[int]) -> str:
    if expires_at is None:
        return "Навсегда"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expires_at))


def describe_license(row: Optional[sqlite3.Row]) -> str:
    if row is None:
        return "Подписки пока нет."
    bot_username = row["bot_username"] or "-"
    status_map = {
        "active": "Активна",
        "pending_token": "Ожидает токен",
        "expired": "Истекла",
        "stopped": "Остановлена",
    }
    status = status_map.get(str(row["status"]), str(row["status"]))
    plan_map = {
        "week": "Неделя",
        "month": "Месяц",
        "lifetime": "Навсегда",
    }
    plan_name = plan_map.get(str(row["plan_key"]), str(row["plan_key"]))
    return (
        f"Статус: {status}\n"
        f"Бот: @{bot_username}\n"
        f"Тариф: {plan_name}\n"
        f"Действует до: {format_expiration(row['expires_at'])}"
    )
