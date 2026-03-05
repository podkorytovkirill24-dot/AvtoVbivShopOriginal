class MiniAppHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in ("/miniapp", "/miniapp/"):
            html = build_miniapp_html().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)
            return
        if path == "/miniapp/health":
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path not in ("/miniapp/api/me", "/miniapp/api/submit", "/miniapp/api/withdraw", "/miniapp/api/admin/payout"):
            self.send_response(404)
            self.end_headers()
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            size = 0
        raw = self.rfile.read(min(size, 16384))
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            self.send_response(400)
            self.end_headers()
            return
        init_data = str(payload.get("init_data", ""))
        tg_user = verify_telegram_webapp_init_data(init_data)
        if not tg_user:
            self.send_response(401)
            self.end_headers()
            return
        if path == "/miniapp/api/me":
            result = build_miniapp_user_payload(int(tg_user["id"]))
        elif path == "/miniapp/api/submit":
            try:
                tariff_id = int(payload.get("tariff_id"))
            except Exception:
                self.send_response(400)
                self.end_headers()
                return
            numbers_text = str(payload.get("numbers_text", ""))
            result = submit_numbers_from_miniapp(
                tg_user=tg_user,
                numbers_text=numbers_text,
                tariff_id=tariff_id,
            )
        elif path == "/miniapp/api/withdraw":
            result = create_withdraw_request_from_miniapp(tg_user=tg_user, amount_value=payload.get("amount"))
        else:
            result = create_payout_from_miniapp_admin(
                tg_admin=tg_user,
                target_raw=str(payload.get("target", "")),
                amount_value=payload.get("amount"),
                note=str(payload.get("note", "")),
            )
        body = json.dumps(result, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        return
