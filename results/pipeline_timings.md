| Файл | Запрос | Время, с |
|---|---|---|
| 01_bronze.sql | `SET SESSION lakehouse.compression_codec = 'ZSTD'` | 0.03 |
| 01_bronze.sql | `CREATE SCHEMA IF NOT EXISTS lakehouse.bronze` | 0.02 |
| 01_bronze.sql | `DROP TABLE IF EXISTS lakehouse.bronze.region` | 0.05 |
| 01_bronze.sql | `DROP TABLE IF EXISTS lakehouse.bronze.nation` | 0.01 |
| 01_bronze.sql | `DROP TABLE IF EXISTS lakehouse.bronze.customer` | 0.01 |
| 01_bronze.sql | `DROP TABLE IF EXISTS lakehouse.bronze.orders` | 0.01 |
| 01_bronze.sql | `DROP TABLE IF EXISTS lakehouse.bronze.lineitem` | 0.01 |
| 01_bronze.sql | `CREATE TABLE lakehouse.bronze.region WITH (format = 'PARQ...` | 2.02 |
| 01_bronze.sql | `CREATE TABLE lakehouse.bronze.nation WITH (format = 'PARQ...` | 0.30 |
| 01_bronze.sql | `CREATE TABLE lakehouse.bronze.customer WITH (format = 'PA...` | 1.13 |
| 01_bronze.sql | `CREATE TABLE lakehouse.bronze.orders WITH (format = 'PARQ...` | 2.93 |
| 01_bronze.sql | `CREATE TABLE lakehouse.bronze.lineitem WITH (format = 'PA...` | 8.39 |
| 02_silver.sql | `SET SESSION lakehouse.compression_codec = 'ZSTD'` | 0.02 |
| 02_silver.sql | `CREATE SCHEMA IF NOT EXISTS lakehouse.silver` | 0.02 |
| 02_silver.sql | `DROP TABLE IF EXISTS lakehouse.silver.customer` | 0.02 |
| 02_silver.sql | `DROP TABLE IF EXISTS lakehouse.silver.order_lines` | 0.02 |
| 02_silver.sql | `CREATE TABLE lakehouse.silver.customer WITH (format = 'PA...` | 2.82 |
| 02_silver.sql | `CREATE TABLE lakehouse.silver.order_lines WITH ( format =...` | 25.81 |
| 03_gold.sql | `SET SESSION lakehouse.compression_codec = 'ZSTD'` | 0.03 |
| 03_gold.sql | `CREATE SCHEMA IF NOT EXISTS lakehouse.gold` | 0.02 |
| 03_gold.sql | `DROP TABLE IF EXISTS lakehouse.gold.q10_returned_customers` | 0.02 |
| 03_gold.sql | `DROP TABLE IF EXISTS lakehouse.gold.returns_by_nation_qua...` | 0.02 |
| 03_gold.sql | `CREATE TABLE lakehouse.gold.q10_returned_customers WITH (...` | 1.56 |
| 03_gold.sql | `CREATE TABLE lakehouse.gold.returns_by_nation_quarter WIT...` | 3.44 |
| 04_checks.sql | `SELECT 'bronze.customer' AS t, count(*) AS cnt FROM lakeh...` | 0.60 |
| 04_checks.sql | `WITH ref AS ( SELECT c.custkey, round(sum(l.extendedprice...` | 3.98 |
