import os
import re
import time

import trino

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
SF = os.getenv("SF", "sf1")


def connect():
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="student", catalog="lakehouse"
    )


def split_sql(text):
    """Разбивает SQL-файл на отдельные запросы, убирая комментарии."""
    text = text.replace("${SF}", SF)
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def run(cur, sql):
    """Выполняет запрос, возвращает (строки, колонки, wall-время сек, статистику Trino)."""
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    elapsed = time.perf_counter() - t0
    cols = [d[0] for d in cur.description] if cur.description else []
    return rows, cols, elapsed, cur.stats


def short(sql, n=70):
    s = re.sub(r"\s+", " ", sql)
    return s if len(s) <= n else s[: n - 3] + "..."
