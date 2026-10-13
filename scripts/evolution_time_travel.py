"""Schema evolution и time travel на silver.customer.

Сценарий:
  1) запоминаем текущий снапшот;
  2) ADD COLUMN customer_tier, заполняем UPDATE'ом (новый снапшот);
  3) RENAME COLUMN phone -> phone_number и обратно;
  4) "случайно" удаляем клиентов одной страны;
  5) читаем таблицу в прошлом (FOR VERSION AS OF / FOR TIMESTAMP AS OF);
  6) восстанавливаем таблицу откатом на снапшот до удаления.
Весь вывод пишется в results/evolution_time_travel.md.
"""
from pathlib import Path

from common import connect, run

ROOT = Path(__file__).resolve().parent.parent
T = "lakehouse.silver.customer"
out = []


def q(cur, sql, title=None, show=True):
    rows, cols, sec, _ = run(cur, sql)
    if title:
        out.append(f"\n**{title}**\n")
    out.append("```sql\n" + sql.strip() + "\n```")
    if show and cols and rows:
        out.append("| " + " | ".join(cols) + " |")
        out.append("|" + "---|" * len(cols))
        for r in rows[:15]:
            out.append("| " + " | ".join(str(v) for v in r) + " |")
    print(sql.strip().splitlines()[0], "->", rows[:3] if rows else "ok")
    return rows


def current_snapshot(cur):
    return run(cur, f'SELECT snapshot_id FROM lakehouse.silver."customer$snapshots" ORDER BY committed_at DESC LIMIT 1')[0][0][0]


def main():
    cur = connect().cursor()

    out.append("## Schema evolution\n")
    s0 = current_snapshot(cur)
    q(cur, f"SELECT count(*) AS rows FROM {T}", f"Исходное состояние, снапшот {s0}")
    q(cur, f'SELECT count(*) AS data_files FROM lakehouse.silver."customer$files"', "Число data-файлов до изменений")

    q(cur, f"ALTER TABLE {T} ADD COLUMN customer_tier varchar", "Добавляем колонку", show=False)
    q(cur, f"SELECT custkey, name, acctbal, customer_tier FROM {T} ORDER BY custkey LIMIT 3",
      "Старые файлы не переписаны — новая колонка читается как NULL")
    q(cur, f'SELECT count(*) AS data_files FROM lakehouse.silver."customer$files"',
      "Число data-файлов после ADD COLUMN (не изменилось: меняются только метаданные)")

    q(cur, f"""
UPDATE {T}
SET customer_tier = CASE
    WHEN acctbal >= 7000 THEN 'GOLD'
    WHEN acctbal >= 3000 THEN 'SILVER'
    WHEN acctbal >= 0    THEN 'BRONZE'
    ELSE 'DEBT' END""", "Заполняем колонку (новый снапшот)", show=False)
    q_tiers = f"SELECT customer_tier, count(*) AS customers FROM {T} GROUP BY 1 ORDER BY 1"
    q(cur, q_tiers, "Распределение по новой колонке")

    q(cur, f"ALTER TABLE {T} RENAME COLUMN phone TO phone_number", "Переименование колонки", show=False)
    q(cur, f"SELECT custkey, phone_number FROM {T} ORDER BY custkey LIMIT 3",
      "Данные читаются под новым именем: Iceberg связывает колонки по field-id, а не по имени")
    q(cur, f"ALTER TABLE {T} RENAME COLUMN phone_number TO phone",
      "Возвращаем имя, чтобы не ломать gold", show=False)

    out.append("\n## Time travel и восстановление\n")
    s_before_delete = current_snapshot(cur)
    q(cur, f"SELECT count(*) AS rows FROM {T}", f"Снапшот перед «аварией»: {s_before_delete}")
    q(cur, f"DELETE FROM {T} WHERE nation_name = 'RUSSIA'", "«Случайное» удаление клиентов из RUSSIA", show=False)
    q(cur, f"SELECT count(*) AS rows, count_if(nation_name = 'RUSSIA') AS russia FROM {T}", "После удаления")
    q(cur, f'SELECT snapshot_id, committed_at, operation FROM lakehouse.silver."customer$snapshots" ORDER BY committed_at',
      "История снапшотов")

    q(cur, f"SELECT count(*) AS rows, count_if(nation_name = 'RUSSIA') AS russia FROM {T} FOR VERSION AS OF {s_before_delete}",
      "Чтение таблицы в прошлом по id снапшота")
    q(cur, f"SELECT count(*) AS rows FROM {T} FOR VERSION AS OF {s0}",
      "Самая первая версия таблицы")

    q(cur, f"ALTER TABLE {T} EXECUTE rollback_to_snapshot({s_before_delete})",
      "Восстановление: откат текущего состояния на снапшот до удаления", show=False)
    q(cur, f"SELECT count(*) AS rows, count_if(nation_name = 'RUSSIA') AS russia FROM {T}", "После отката")

    (ROOT / "results" / "evolution_time_travel.md").write_text("\n".join(out) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
