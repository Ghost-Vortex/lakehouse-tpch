-- GOLD: витрины для бизнес-аналитики.

SET SESSION lakehouse.compression_codec = 'ZSTD';

CREATE SCHEMA IF NOT EXISTS lakehouse.gold;

DROP TABLE IF EXISTS lakehouse.gold.q10_returned_customers;
DROP TABLE IF EXISTS lakehouse.gold.returns_by_nation_quarter;

-- 1) Ответ на вариант 2 (TPC-H Q10): топ-20 клиентов по выручке
--    от возвращённых позиций за 4-й квартал 1993 года.
CREATE TABLE lakehouse.gold.q10_returned_customers
WITH (format = 'PARQUET')
AS
SELECT
    c.custkey,
    c.name,
    CAST(sum(ol.net_revenue) AS decimal(18, 4)) AS revenue,
    c.acctbal,
    c.nation_name AS nation,
    c.address,
    c.phone,
    c.comment
FROM lakehouse.silver.order_lines ol
JOIN lakehouse.silver.customer    c ON c.custkey = ol.custkey
WHERE ol.orderdate >= DATE '1993-10-01'
  AND ol.orderdate <  DATE '1994-01-01'
  AND ol.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, c.nation_name, c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20;

-- 2) Агрегат для дашборда: возвраты по странам и кварталам
--    (позволяет смотреть динамику, а не один квартал).
CREATE TABLE lakehouse.gold.returns_by_nation_quarter
WITH (format = 'PARQUET')
AS
SELECT
    c.region_name,
    c.nation_name,
    ol.order_quarter,
    count(*)                                    AS returned_lines,
    count(DISTINCT ol.custkey)                  AS customers,
    count(DISTINCT ol.orderkey)                 AS orders,
    CAST(sum(ol.net_revenue) AS decimal(18, 2)) AS returned_revenue
FROM lakehouse.silver.order_lines ol
JOIN lakehouse.silver.customer    c ON c.custkey = ol.custkey
WHERE ol.returnflag = 'R'
GROUP BY c.region_name, c.nation_name, ol.order_quarter;
