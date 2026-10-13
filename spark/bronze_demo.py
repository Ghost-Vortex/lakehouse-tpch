"""Spark подключается к тому же Iceberg REST-каталогу (Lakekeeper) и читает
bronze-таблицы, которые были записаны Trino. Никакого копирования данных:
Spark сам находит metadata.json через каталог и читает parquet из MinIO.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bronze-demo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def section(title):
    print("\n" + "=" * 70 + f"\n{title}\n" + "=" * 70)


section("1. Пространства имён в каталоге lakehouse")
spark.sql("SHOW NAMESPACES IN lakehouse").show()

section("2. Таблицы bronze")
spark.sql("SHOW TABLES IN lakehouse.bronze").show()

section("3. Схема bronze.lineitem (её создал Trino)")
spark.table("lakehouse.bronze.lineitem").printSchema()

section("4. Количество строк в bronze")
for t in ["region", "nation", "customer", "orders", "lineitem"]:
    print(f"bronze.{t:9s} {spark.table(f'lakehouse.bronze.{t}').count():>10,}")

section("5. Снапшоты bronze.lineitem (метаданные Iceberg)")
spark.sql(
    "SELECT committed_at, snapshot_id, operation, summary['added-records'] AS added_records "
    "FROM lakehouse.bronze.lineitem.snapshots"
).show(truncate=False)

section("6. Data-файлы bronze.lineitem в MinIO")
spark.sql(
    "SELECT file_path, file_format, record_count, "
    "round(file_size_in_bytes / 1024 / 1024, 2) AS size_mb "
    "FROM lakehouse.bronze.lineitem.files"
).show(truncate=False)

section("7. Q10 (вариант 2), посчитанный Spark прямо по bronze")
q10 = spark.sql("""
    SELECT c.custkey, c.name,
           round(sum(l.extendedprice * (1 - l.discount)), 2) AS revenue,
           n.name AS nation
    FROM lakehouse.bronze.customer c
    JOIN lakehouse.bronze.orders   o ON c.custkey = o.custkey
    JOIN lakehouse.bronze.lineitem l ON l.orderkey = o.orderkey
    JOIN lakehouse.bronze.nation   n ON c.nationkey = n.nationkey
    WHERE o.orderdate >= DATE '1993-10-01' AND o.orderdate < DATE '1994-01-01'
      AND l.returnflag = 'R'
    GROUP BY c.custkey, c.name, n.name
    ORDER BY revenue DESC
    LIMIT 20
""")
q10.show(truncate=False)

section("8. Сверка со витриной gold, построенной Trino")
gold = spark.table("lakehouse.gold.q10_returned_customers").selectExpr(
    "custkey", "round(cast(revenue AS double), 2) AS gold_revenue"
)
diff = q10.join(gold, "custkey", "full_outer").where(
    "revenue IS NULL OR gold_revenue IS NULL OR abs(revenue - gold_revenue) > 0.01"
)
print(f"расхождений: {diff.count()}")

spark.stop()
