#!/usr/bin/env python3
"""Веб-дашборд (FastAPI) + бот в одном процессе и одном event loop.

Запуск:  python server.py          (в Docker это делает CMD)
Настройка — переменными окружения (см. .env):
    ARB_ARGS            аргументы бота, как в командной строке arbbot.py
    DASHBOARD_USER / DASHBOARD_PASSWORD    вход в дашборд (пароль пуст -> генерируется и печатается в лог)
    DASHBOARD_TITLE / DASHBOARD_HOST / DASHBOARD_PORT
"""
from __future__ import annotations

import asyncio
import html
import logging
import os
import secrets
import shlex
from pathlib import Path

import arbbot

log = logging.getLogger("arb")
HERE = Path(__file__).resolve().parent


def create_app(rt: "arbbot.Runtime", user: str, password: str, title: str):
    from fastapi import Depends, FastAPI, HTTPException, Request
    from fastapi.responses import HTMLResponse, JSONResponse
    from fastapi.security import HTTPBasic, HTTPBasicCredentials

    app = FastAPI(title=title, docs_url=None, redoc_url=None, openapi_url=None)
    security = HTTPBasic(realm="Arbitrage Bot")
    page = (HERE / "templates" / "dashboard.html").read_text(encoding="utf-8").replace("__TITLE__", html.escape(title))

    def auth(creds: HTTPBasicCredentials = Depends(security)) -> None:
        ok = secrets.compare_digest(creds.username.encode(), user.encode()) & \
             secrets.compare_digest(creds.password.encode(), password.encode())
        if not ok:
            raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": 'Basic realm="Arbitrage Bot"'})

    def require_xhr(request: Request) -> None:
        # защита от CSRF: такой заголовок нельзя выставить со стороннего сайта без CORS-предзапроса
        if request.headers.get("x-requested-with") != "dashboard":
            raise HTTPException(status_code=403, detail="Forbidden")

    # Все обработчики — async: выполняются в потоке event loop, где живёт бот (без гонок данных).
    @app.get("/health")
    async def health():
        return {"ok": True, "status": rt.status(), "uptime_s": round(__import__("time").time() - rt.started_at)}

    @app.get("/", response_class=HTMLResponse)
    async def index(_: None = Depends(auth)):
        return HTMLResponse(page)

    @app.get("/api/status")
    async def api_status(_: None = Depends(auth)):
        return JSONResponse(rt.snapshot())

    @app.post("/api/pause")
    async def api_pause(request: Request, _: None = Depends(auth)):
        require_xhr(request)
        rt.pause()
        return {"status": rt.status()}

    @app.post("/api/resume")
    async def api_resume(request: Request, _: None = Depends(auth)):
        require_xhr(request)
        rt.resume()
        return {"status": rt.status()}

    return app


async def main() -> None:
    cfg = arbbot.parse_args(shlex.split(os.environ.get("ARB_ARGS", "--mode sim --duration 0")))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03d %(message)s", datefmt="%H:%M:%S")

    user = os.environ.get("DASHBOARD_USER") or "admin"
    password = os.environ.get("DASHBOARD_PASSWORD") or ""
    generated = not password
    if generated:
        password = secrets.token_urlsafe(12)

    rt = arbbot.Runtime(cfg)
    app = create_app(rt, user, password, os.environ.get("DASHBOARD_TITLE") or "Arbitrage Bot")
    await rt.start()
    port = int(os.environ.get("DASHBOARD_PORT_INTERNAL", "8080"))
    log.info("Дашборд: http://<сервер>:%s  логин: %s", os.environ.get("DASHBOARD_PORT", "8080"), user)
    if generated:
        log.warning("DASHBOARD_PASSWORD не задан — временный пароль для '%s': %s  (задайте постоянный в .env)", user, password)

    import uvicorn
    server = uvicorn.Server(uvicorn.Config(app, host=os.environ.get("DASHBOARD_HOST", "0.0.0.0"), port=port,
                                           log_level="warning", access_log=False))
    try:
        await server.serve()  # uvicorn сам ловит SIGINT/SIGTERM и завершается штатно
    finally:
        await rt.stop()
        log.info("бот остановлен")


if __name__ == "__main__":
    if arbbot.uvloop:
        arbbot.uvloop.install()
    asyncio.run(main())