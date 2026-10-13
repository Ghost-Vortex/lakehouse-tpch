-- BRONZE: сырые данные "как есть" из генератора TPC-H.
-- Берём все колонки нужных таблиц без изменений — bronze служит
-- неизменяемой копией источника, из которой можно пересобрать silver/gold.
-- Формат PARQUET, сжатие ZSTD (задаётся сессией), без партиционирования.

SET SESSION lakehouse.compression_codec = 'ZSTD';

CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;

DROP TABLE IF EXISTS lakehouse.bronze.region;
DROP TABLE IF EXISTS lakehouse.bronze.nation;
DROP TABLE IF EXISTS lakehouse.bronze.customer;
DROP TABLE IF EXISTS lakehouse.bronze.orders;
DROP TABLE IF EXISTS lakehouse.bronze.lineitem;

CREATE TABLE lakehouse.bronze.region   WITH (format = 'PARQUET') AS SELECT * FROM tpch.${SF}.region;
CREATE TABLE lakehouse.bronze.nation   WITH (format = 'PARQUET') AS SELECT * FROM tpch.${SF}.nation;
CREATE TABLE lakehouse.bronze.customer WITH (format = 'PARQUET') AS SELECT * FROM tpch.${SF}.customer;
CREATE TABLE lakehouse.bronze.orders   WITH (format = 'PARQUET') AS SELECT * FROM tpch.${SF}.orders;
CREATE TABLE lakehouse.bronze.lineitem WITH (format = 'PARQUET') AS SELECT * FROM tpch.${SF}.lineitem;
