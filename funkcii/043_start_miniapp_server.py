def start_miniapp_server() -> None:
    if MINI_APP_PORT <= 0:
        return
    try:
        server = ThreadingHTTPServer((MINI_APP_HOST, MINI_APP_PORT), MiniAppHandler)
    except Exception as exc:
        logger.warning("MiniApp server start failed: %s", exc)
        return
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="miniapp-http")
    thread.start()
    logger.info("MiniApp HTTP listening on http://%s:%s", MINI_APP_HOST, MINI_APP_PORT)
