-- SILVER: очищенные, нормализованные и обогащённые данные.
--   * очистка: trim строк, отсев строк с пустыми ключами и некорректными
--     значениями (цена <= 0, скидка вне [0;1]), дедупликация по ключу;
--   * нормализация: денежные поля double -> decimal, флаги к верхнему регистру;
--   * обогащение: клиенту добавлены страна и регион, строке заказа — custkey,
--     дата/квартал заказа и чистая выручка net_revenue = extendedprice * (1 - discount).

SET SESSION lakehouse.compression_codec = 'ZSTD';

CREATE SCHEMA IF NOT EXISTS lakehouse.silver;

DROP TABLE IF EXISTS lakehouse.silver.customer;
DROP TABLE IF EXISTS lakehouse.silver.order_lines;

-- Справочник клиентов (150k строк на sf1) — маленький, партиционировать незачем
CREATE TABLE lakehouse.silver.customer
WITH (format = 'PARQUET')
AS
WITH dedup AS (
    SELECT c.*,
           row_number() OVER (PARTITION BY c.custkey ORDER BY c.custkey) AS rn
    FROM lakehouse.bronze.customer c
    WHERE c.custkey IS NOT NULL
)
SELECT
    d.custkey,
    trim(d.name)                       AS name,
    trim(d.address)                    AS address,
    trim(d.phone)                      AS phone,
    CAST(d.acctbal AS decimal(15, 2))  AS acctbal,
    upper(trim(d.mktsegment))          AS mktsegment,
    trim(d.comment)                    AS comment,
    d.nationkey,
    trim(n.name)                       AS nation_name,
    trim(r.name)                       AS region_name
FROM dedup d
LEFT JOIN lakehouse.bronze.nation n ON n.nationkey = d.nationkey
LEFT JOIN lakehouse.bronze.region r ON r.regionkey = n.regionkey
WHERE d.rn = 1;

-- Факт "строки заказов": orders + lineitem в одной широкой таблице.
-- Партиционирование по году заказа (7 партиций на 1992-1998) — фильтр
-- по дате в gold отсекает лишние партиции; внутри файлов сортировка по
-- returnflag, orderdate, чтобы min/max-статистики Parquet пропускали row group'ы.
CREATE TABLE lakehouse.silver.order_lines
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['year(orderdate)'],
    sorted_by    = ARRAY['returnflag', 'orderdate']
)
AS
SELECT
    l.orderkey,
    l.linenumber,
    o.custkey,
    o.orderdate,
    CAST(date_trunc('quarter', o.orderdate) AS date)            AS order_quarter,
    upper(trim(o.orderstatus))                                  AS orderstatus,
    upper(trim(l.returnflag))                                   AS returnflag,
    upper(trim(l.linestatus))                                   AS linestatus,
    l.shipdate,
    CAST(l.quantity      AS decimal(15, 2))                     AS quantity,
    CAST(l.extendedprice AS decimal(15, 2))                     AS extendedprice,
    CAST(l.discount      AS decimal(15, 2))                     AS discount,
    CAST(l.extendedprice * (1 - l.discount) AS decimal(18, 4))  AS net_revenue
FROM lakehouse.bronze.lineitem l
JOIN lakehouse.bronze.orders   o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND o.custkey  IS NOT NULL
  AND l.extendedprice > 0
  AND l.discount BETWEEN 0 AND 1;
