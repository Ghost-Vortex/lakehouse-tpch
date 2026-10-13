"""Строки, размер и число файлов по каждой таблице bronze/silver/gold.

Размер берётся из метаданных Iceberg (таблица "<t>$files"), т.е. это
реальный объём data-файлов в MinIO без metadata/manifest-файлов.
"""
from pathlib import Path

from common import connect, run

ROOT = Path(__file__).resolve().parent.parent
LAYERS = ["bronze", "silver", "gold"]


def main():
    cur = connect().cursor()
    lines = ["| Слой | Таблица | Строк | Файлов | Размер, МБ |", "|---|---|---|---|---|"]
    for layer in LAYERS:
        tables, *_ = run(cur, f"SHOW TABLES FROM lakehouse.{layer}")
        for (t,) in sorted(tables):
            (cnt,), = run(cur, f"SELECT count(*) FROM lakehouse.{layer}.{t}")[0]
            (files, size), = run(
                cur,
                f'SELECT count(*), coalesce(sum(file_size_in_bytes), 0) '
                f'FROM lakehouse.{layer}."{t}$files"',
            )[0]
            row = f"| {layer} | {t} | {cnt:,} | {files} | {size / 1024 / 1024:.3f} |"
            lines.append(row.replace(",", " "))
            print(row)
    (ROOT / "results" / "layer_stats.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
