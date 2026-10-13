"""Время ответа на вопрос варианта 2 на разных слоях медальона:
Q10 по сырому bronze, тот же запрос по silver и чтение готовой витрины gold."""
import statistics
from pathlib import Path

from benchmark import Q10
from common import connect, run

ROOT = Path(__file__).resolve().parent.parent
RUNS = 11

SILVER_Q10 = """
SELECT c.custkey, c.name, sum(ol.net_revenue) AS revenue, c.acctbal,
       c.nation_name, c.address, c.phone, c.comment
FROM lakehouse.silver.order_lines ol
JOIN lakehouse.silver.customer c ON c.custkey = ol.custkey
WHERE ol.orderdate >= DATE '1993-10-01' AND ol.orderdate < DATE '1994-01-01'
  AND ol.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, c.nation_name, c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20
"""
GOLD = "SELECT * FROM lakehouse.gold.q10_returned_customers ORDER BY revenue DESC"

CASES = [
    ("Q10 по bronze (4 таблицы, полный скан)", Q10.format(s="lakehouse.bronze")),
    ("Q10 по silver (2 таблицы, партиция 1993)", SILVER_Q10),
    ("Чтение gold-витрины", GOLD),
]


def main():
    cur = connect().cursor()
    lines = ["| Вариант | Медиана, с | Мин, с | Обработано строк | Обработано, МБ |", "|---|---|---|---|---|"]
    for title, sql in CASES:
        times, stats = [], None
        for i in range(RUNS):
            _, _, sec, stats = run(cur, sql)
            if i:
                times.append(sec)
        line = (f"| {title} | {statistics.median(times):.3f} | {min(times):.3f} "
                f"| {stats.get('processedRows', 0):,} | {stats.get('processedBytes', 0) / 1024 / 1024:.1f} |")
        print(line)
        lines.append(line.replace(",", " "))
    (ROOT / "results" / "query_timing.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
