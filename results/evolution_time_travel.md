## Schema evolution


**Исходное состояние, снапшот 7609793447781206439**

```sql
SELECT count(*) AS rows FROM lakehouse.silver.customer
```
| rows |
|---|
| 150000 |

**Число data-файлов до изменений**

```sql
SELECT count(*) AS data_files FROM lakehouse.silver."customer$files"
```
| data_files |
|---|
| 1 |

**Добавляем колонку**

```sql
ALTER TABLE lakehouse.silver.customer ADD COLUMN customer_tier varchar
```

**Старые файлы не переписаны — новая колонка читается как NULL**

```sql
SELECT custkey, name, acctbal, customer_tier FROM lakehouse.silver.customer ORDER BY custkey LIMIT 3
```
| custkey | name | acctbal | customer_tier |
|---|---|---|---|
| 1 | Customer#000000001 | 711.56 | None |
| 2 | Customer#000000002 | 121.65 | None |
| 3 | Customer#000000003 | 7498.12 | None |

**Число data-файлов после ADD COLUMN (не изменилось: меняются только метаданные)**

```sql
SELECT count(*) AS data_files FROM lakehouse.silver."customer$files"
```
| data_files |
|---|
| 1 |

**Заполняем колонку (новый снапшот)**

```sql
UPDATE lakehouse.silver.customer
SET customer_tier = CASE
    WHEN acctbal >= 7000 THEN 'GOLD'
    WHEN acctbal >= 3000 THEN 'SILVER'
    WHEN acctbal >= 0    THEN 'BRONZE'
    ELSE 'DEBT' END
```

**Распределение по новой колонке**

```sql
SELECT customer_tier, count(*) AS customers FROM lakehouse.silver.customer GROUP BY 1 ORDER BY 1
```
| customer_tier | customers |
|---|---|
| BRONZE | 41044 |
| DEBT | 13692 |
| GOLD | 40928 |
| SILVER | 54336 |

**Переименование колонки**

```sql
ALTER TABLE lakehouse.silver.customer RENAME COLUMN phone TO phone_number
```

**Данные читаются под новым именем: Iceberg связывает колонки по field-id, а не по имени**

```sql
SELECT custkey, phone_number FROM lakehouse.silver.customer ORDER BY custkey LIMIT 3
```
| custkey | phone_number |
|---|---|
| 1 | 25-989-741-2988 |
| 2 | 23-768-687-3665 |
| 3 | 11-719-748-3364 |

**Возвращаем имя, чтобы не ломать gold**

```sql
ALTER TABLE lakehouse.silver.customer RENAME COLUMN phone_number TO phone
```

## Time travel и восстановление


**Снапшот перед «аварией»: 4142395721619811310**

```sql
SELECT count(*) AS rows FROM lakehouse.silver.customer
```
| rows |
|---|
| 150000 |

**«Случайное» удаление клиентов из RUSSIA**

```sql
DELETE FROM lakehouse.silver.customer WHERE nation_name = 'RUSSIA'
```

**После удаления**

```sql
SELECT count(*) AS rows, count_if(nation_name = 'RUSSIA') AS russia FROM lakehouse.silver.customer
```
| rows | russia |
|---|---|
| 143922 | 0 |

**История снапшотов**

```sql
SELECT snapshot_id, committed_at, operation FROM lakehouse.silver."customer$snapshots" ORDER BY committed_at
```
| snapshot_id | committed_at | operation |
|---|---|---|
| 7609793447781206439 | 2026-10-13 15:06:38.064000+03:00 | append |
| 4142395721619811310 | 2026-10-13 15:36:46.301000+03:00 | overwrite |
| 5626287416498440026 | 2026-10-13 15:36:47.004000+03:00 | delete |

**Чтение таблицы в прошлом по id снапшота**

```sql
SELECT count(*) AS rows, count_if(nation_name = 'RUSSIA') AS russia FROM lakehouse.silver.customer FOR VERSION AS OF 4142395721619811310
```
| rows | russia |
|---|---|
| 150000 | 6078 |

**Самая первая версия таблицы**

```sql
SELECT count(*) AS rows FROM lakehouse.silver.customer FOR VERSION AS OF 7609793447781206439
```
| rows |
|---|
| 150000 |

**Восстановление: откат текущего состояния на снапшот до удаления**

```sql
ALTER TABLE lakehouse.silver.customer EXECUTE rollback_to_snapshot(4142395721619811310)
```

**После отката**

```sql
SELECT count(*) AS rows, count_if(nation_name = 'RUSSIA') AS russia FROM lakehouse.silver.customer
```
| rows | russia |
|---|---|
| 150000 | 6078 |
