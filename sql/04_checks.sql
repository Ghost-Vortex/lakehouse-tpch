-- Проверки: количество строк по слоям и сверка gold с эталонным Q10,
-- выполненным напрямую по генератору tpch.

SELECT 'bronze.customer' AS t, count(*) AS cnt FROM lakehouse.bronze.customer
UNION ALL SELECT 'bronze.orders',   count(*) FROM lakehouse.bronze.orders
UNION ALL SELECT 'bronze.lineitem', count(*) FROM lakehouse.bronze.lineitem
UNION ALL SELECT 'bronze.nation',   count(*) FROM lakehouse.bronze.nation
UNION ALL SELECT 'bronze.region',   count(*) FROM lakehouse.bronze.region
UNION ALL SELECT 'silver.customer', count(*) FROM lakehouse.silver.customer
UNION ALL SELECT 'silver.order_lines', count(*) FROM lakehouse.silver.order_lines
UNION ALL SELECT 'gold.q10_returned_customers', count(*) FROM lakehouse.gold.q10_returned_customers
UNION ALL SELECT 'gold.returns_by_nation_quarter', count(*) FROM lakehouse.gold.returns_by_nation_quarter;

-- Расхождения gold vs эталон (ожидаем 0 строк)
WITH ref AS (
    SELECT c.custkey, round(sum(l.extendedprice * (1 - l.discount)), 2) AS revenue
    FROM tpch.${SF}.customer c
    JOIN tpch.${SF}.orders   o ON c.custkey = o.custkey
    JOIN tpch.${SF}.lineitem l ON l.orderkey = o.orderkey
    JOIN tpch.${SF}.nation   n ON c.nationkey = n.nationkey
    WHERE o.orderdate >= DATE '1993-10-01'
      AND o.orderdate <  DATE '1994-01-01'
      AND l.returnflag = 'R'
    GROUP BY c.custkey
    ORDER BY revenue DESC
    LIMIT 20
)
SELECT coalesce(r.custkey, g.custkey) AS custkey, r.revenue AS ref_revenue, g.revenue AS gold_revenue
FROM ref r
FULL JOIN lakehouse.gold.q10_returned_customers g ON g.custkey = r.custkey
WHERE r.custkey IS NULL OR g.custkey IS NULL
   OR abs(r.revenue - CAST(g.revenue AS double)) > 0.01;
