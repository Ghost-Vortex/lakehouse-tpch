"""Прогон ELT: bronze -> silver -> gold -> проверки.

Использование:  python scripts/run_pipeline.py [файлы.sql ...]
По умолчанию выполняет sql/01..04 по порядку.
"""
import sys
from pathlib import Path

from common import SF, connect, run, short, split_sql

ROOT = Path(__file__).resolve().parent.parent
DEFAULT = ["01_bronze.sql", "02_silver.sql", "03_gold.sql", "04_checks.sql"]


def main():
    files = sys.argv[1:] or [str(ROOT / "sql" / f) for f in DEFAULT]
    cur = connect().cursor()
    log = []
    print(f"scale = {SF}")
    for f in files:
        print(f"\n=== {Path(f).name} ===")
        for stmt in split_sql(Path(f).read_text(encoding="utf-8")):
            rows, cols, sec, stats = run(cur, stmt)
            print(f"[{sec:7.2f}s] {short(stmt)}")
            log.append(f"| {Path(f).name} | `{short(stmt, 60)}` | {sec:.2f} |")
            if stmt.upper().startswith(("SELECT", "WITH", "SHOW")):
                print("   " + " | ".join(cols))
                for r in rows[:40]:
                    print("   " + " | ".join(str(v) for v in r))
                if not rows:
                    print("   (0 rows)")
    out = ROOT / "results" / "pipeline_timings.md"
    out.write_text(
        "| Файл | Запрос | Время, с |\n|---|---|---|\n" + "\n".join(log) + "\n",
        encoding="utf-8",
    )
    print(f"\nтайминги сохранены в {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
