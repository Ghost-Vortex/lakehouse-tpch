"""Бенчмарк форматов и кодеков сжатия на слое bronze.

Для каждой пары (формат, кодек) bronze-таблицы (customer, orders, lineitem,
nation) пересоздаются в отдельной схеме lakehouse.bench_<format>_<codec>, после
чего замеряются:
  * время записи (CTAS) — суммарно по всем таблицам;
  * размер data-файлов (из метаданных Iceberg "$files");
  * время запроса Q10 (вариант 2) и время "тяжёлой" агрегации полного скана
    lineitem (в духе TPC-H Q1).
Каждый запрос гоняется RUNS раз, первый прогон — прогрев и в расчёт не идёт,
берётся медиана остальных. Перед всеми замерами JVM Trino прогревается
запросами по основному bronze, иначе первые конфигурации проигрывают из-за JIT.
Весь набор конфигураций прогоняется ROUNDS раз в случайном порядке, в итог
идёт медиана по раундам (и разброс min..max для времени запросов) — так
меньше влияет порядок запуска и фоновая нагрузка на ноутбуке.

Источник для записи — основной lakehouse.bronze (уже материализован), а не
генератор tpch: так время записи меньше зависит от CPU на генерацию данных.
"""
import csv
import os
import random
import statistics
from pathlib import Path

from common import connect, run

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / "results"
RUNS = int(os.getenv("RUNS", "11"))
WARMUP = int(os.getenv("WARMUP", "5"))
ROUNDS = int(os.getenv("ROUNDS", "3"))
FORMATS = ["PARQUET", "ORC"]
CODECS = ["NONE", "SNAPPY", "LZ4", "ZSTD", "GZIP"]
TABLES = ["nation", "customer", "orders", "lineitem"]

Q10 = """
SELECT c.custkey, c.name,
       sum(l.extendedprice * (1 - l.discount)) AS revenue,
       c.acctbal, n.name AS nation, c.address, c.phone, c.comment
FROM {s}.customer c
JOIN {s}.orders   o ON c.custkey = o.custkey
JOIN {s}.lineitem l ON l.orderkey = o.orderkey
JOIN {s}.nation   n ON c.nationkey = n.nationkey
WHERE o.orderdate >= DATE '1993-10-01' AND o.orderdate < DATE '1994-01-01'
  AND l.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20
"""

AGG = """
SELECT returnflag, linestatus,
       sum(quantity), sum(extendedprice),
       sum(extendedprice * (1 - discount)),
       sum(extendedprice * (1 - discount) * (1 + tax)),
       avg(discount), count(*)
FROM {s}.lineitem
GROUP BY returnflag, linestatus
ORDER BY returnflag, linestatus
"""


def timed(cur, sql):
    times, cpu = [], []
    for i in range(RUNS):
        _, _, sec, stats = run(cur, sql)
        if i == 0:
            continue
        times.append(sec)
        cpu.append(stats.get("cpuTimeMillis", 0) / 1000)
    return statistics.median(times), statistics.median(cpu)


def bench_one(cur, fmt, codec):
    schema = f"lakehouse.bench_{fmt.lower()}_{codec.lower()}"
    run(cur, f"SET SESSION lakehouse.compression_codec = '{codec}'")
    run(cur, f"CREATE SCHEMA IF NOT EXISTS {schema}")

    write_sec, size = 0.0, 0
    for t in TABLES:
        run(cur, f"DROP TABLE IF EXISTS {schema}.{t}")
        _, _, sec, _ = run(
            cur,
            f"CREATE TABLE {schema}.{t} WITH (format = '{fmt}') "
            f"AS SELECT * FROM lakehouse.bronze.{t}",
        )
        write_sec += sec
        (b,), = run(cur, f'SELECT sum(file_size_in_bytes) FROM {schema}."{t}$files"')[0]
        size += b

    q10_sec, q10_cpu = timed(cur, Q10.format(s=schema))
    agg_sec, agg_cpu = timed(cur, AGG.format(s=schema))

    for t in TABLES:
        run(cur, f"DROP TABLE IF EXISTS {schema}.{t}")
    run(cur, f"DROP SCHEMA IF EXISTS {schema}")

    return {
        "format": fmt,
        "codec": codec,
        "size_mb": round(size / 1024 / 1024, 2),
        "write_s": round(write_sec, 2),
        "q10_s": round(q10_sec, 3),
        "q10_cpu_s": round(q10_cpu, 2),
        "agg_s": round(agg_sec, 3),
        "agg_cpu_s": round(agg_cpu, 2),
    }


def save(rows):
    RES.mkdir(exist_ok=True)
    with open(RES / "benchmark.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)

    base = next(r for r in rows if r["format"] == "PARQUET" and r["codec"] == "NONE")
    md = [
        "| Формат | Кодек | Размер, МБ | Сжатие к PARQUET/NONE | Запись, с "
        "| Q10, с | Q10 разброс | Q10 CPU, с | Агрегация, с | Агрегация разброс | Агрегация CPU, с |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        ratio = base["size_mb"] / r["size_mb"]
        md.append(
            f"| {r['format']} | {r['codec']} | {r['size_mb']} | x{ratio:.2f} | {r['write_s']} "
            f"| {r['q10_s']} | {r['q10_range']} | {r['q10_cpu_s']} "
            f"| {r['agg_s']} | {r['agg_range']} | {r['agg_cpu_s']} |"
        )
    (RES / "benchmark.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))


def plot(rows):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = [f"{r['format'][:3]}\n{r['codec']}" for r in rows]
    metrics = [
        ("size_mb", "Размер bronze, МБ"),
        ("write_s", "Время записи, с"),
        ("q10_s", "Q10, с (медиана)"),
        ("agg_s", "Агрегация lineitem, с (медиана)"),
    ]
    colors = ["#4C72B0" if r["format"] == "PARQUET" else "#DD8452" for r in rows]
    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    for ax, (key, title) in zip(axes.flat, metrics):
        ax.bar(labels, [r[key] for r in rows], color=colors)
        ax.set_title(title)
        ax.tick_params(axis="x", labelsize=8)
        ax.grid(axis="y", alpha=0.3)
    fig.suptitle("Bronze: PARQUET (синий) vs ORC (оранжевый) с разными кодеками")
    fig.tight_layout()
    fig.savefig(RES / "benchmark.png", dpi=120)


def aggregate(results):
    rows = []
    for fmt in FORMATS:
        for codec in CODECS:
            runs = results.get((fmt, codec))
            if not runs:
                continue
            row = {"format": fmt, "codec": codec}
            for key in ["size_mb", "write_s", "q10_s", "q10_cpu_s", "agg_s", "agg_cpu_s"]:
                row[key] = round(statistics.median(r[key] for r in runs), 3)
            row["q10_range"] = f"{min(r['q10_s'] for r in runs):.2f}..{max(r['q10_s'] for r in runs):.2f}"
            row["agg_range"] = f"{min(r['agg_s'] for r in runs):.2f}..{max(r['agg_s'] for r in runs):.2f}"
            rows.append(row)
    return rows


def main():
    cur = connect().cursor()
    for _ in range(WARMUP):
        run(cur, Q10.format(s="lakehouse.bronze"))
        run(cur, AGG.format(s="lakehouse.bronze"))
    configs = [(f, c) for f in FORMATS for c in CODECS]
    results, raw = {}, []
    for rnd in range(1, ROUNDS + 1):
        random.shuffle(configs)
        for fmt, codec in configs:
            try:
                r = bench_one(cur, fmt, codec)
            except Exception as e:  # например, кодек не поддержан для формата
                print(f"round {rnd} {fmt}/{codec}: пропущено — {e}")
                run(cur, f"DROP SCHEMA IF EXISTS lakehouse.bench_{fmt.lower()}_{codec.lower()}")
                continue
            print(f"round {rnd}", r)
            results.setdefault((fmt, codec), []).append(r)
            raw.append({"round": rnd, **r})
    with open(RES / "benchmark_raw.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=raw[0].keys())
        w.writeheader()
        w.writerows(raw)
    rows = aggregate(results)
    save(rows)
    plot(rows)


if __name__ == "__main__":
    main()
