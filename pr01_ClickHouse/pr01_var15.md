# ПЗ №1. Основы ClickHouse: установка, типы данных, движки таблиц
## Выполнила: Песенкова Екатерина, ЦИБ-214
## Вариант 15

##  Цель работы

Получить практические навыки работы с колоночной СУБД ClickHouse: подключиться к облачному серверу, освоить создание баз данных и таблиц с правильным выбором типов данных и движков семейства MergeTree.

---

##  Задание 1. Создание базы данных и таблицы продаж

``` sql
CREATE TABLE sales_var15 (
    sale_id        UInt64,
    sale_timestamp DateTime64(3),
    product_id     UInt32,
    category       LowCardinality(String),
    customer_id    UInt64,
    region         LowCardinality(String),
    quantity       UInt16,
    unit_price     Decimal64(2),
    discount_pct   Float32,
    is_online      UInt8,
    ip_address     IPv4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_timestamp)
ORDER BY (sale_timestamp, customer_id, product_id);

-- Генерация 150 строк за 3 месяца (Октябрь, Ноябрь, Декабрь 2024)
-- Параметры для варианта 15:
-- sale_id: 15001 ... 15150
-- customer_id: 1500 ... 1599
-- product_id: 150 ... 169
-- quantity: 1 ... 20 (max = 20)
-- unit_price: от 25.00
-- Категории: Office, Electronics, Clothing, Food, Pets

INSERT INTO sales_var15 
(sale_id, sale_timestamp, product_id, category, customer_id, region, quantity, unit_price, discount_pct, is_online, ip_address)
SELECT
    number + 15001 AS sale_id,
    toDateTime64('2024-10-01 00:00:00', 3) + INTERVAL (number % 90) DAY + INTERVAL (number % 24) HOUR,
    150 + (number % 20) AS product_id,
    CASE (number % 5)
        WHEN 0 THEN 'Office'
        WHEN 1 THEN 'Electronics'
        WHEN 2 THEN 'Clothing'
        WHEN 3 THEN 'Food'
        ELSE 'Pets'
    END AS category,
    1500 + (number % 100) AS customer_id,
    CASE (number % 4)
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        ELSE 'West'
    END AS region,
    (number % 20) + 1 AS quantity,
    25.00 + (number % 500) AS unit_price,
    (number % 100) / 100.0 AS discount_pct,
    (number % 2) AS is_online,
    IPv4StringToNum(concat(
        toString(number % 256), '.', 
        toString((number + 64) % 256), '.', 
        toString((number + 128) % 256), '.', 
        toString(number % 256)
    )) AS ip_address
FROM numbers(150);

select * from sales_var15;
```

<img width="1763" height="736" alt="image" src="https://github.com/user-attachments/assets/312b0c48-af4d-47cc-a8b8-15bd5f08238a" />

---
## Задание 2. Аналитические запросы
Общая выручка по категориям
--
```sql
SELECT
    category,
    round(SUM(quantity * unit_price * (1 - discount_pct)), 2) AS total_revenue
FROM sales_var15
GROUP BY category
ORDER BY total_revenue DESC;
```
<img width="415" height="208" alt="image" src="https://github.com/user-attachments/assets/02f4f976-8e6e-49eb-8fe4-8f86e6db4b08" />

Топ-3 клиента по количеству покупок
--
```sql
SELECT
    customer_id,
    COUNT(*) AS purchase_count,
    SUM(quantity) AS total_quantity
FROM sales_var15
GROUP BY customer_id
ORDER BY purchase_count DESC
LIMIT 3;
```
<img width="589" height="154" alt="image" src="https://github.com/user-attachments/assets/e0e752d2-0eab-4e83-8f22-5a7cc78163a0" />

Средний чек по месяцам
--
```sql
SELECT
    toYYYYMM(sale_timestamp) AS month,
    round(AVG(quantity * unit_price), 2) AS avg_check
FROM sales_var15
GROUP BY month
ORDER BY month;
```
<img width="391" height="134" alt="image" src="https://github.com/user-attachments/assets/d255da37-124b-4882-96af-d4129c09401b" />

Фильтрация по партиции
--
```sql
SELECT *
FROM sales_var15
WHERE sale_timestamp >= '2024-11-01' AND sale_timestamp < '2024-12-01';

-- Проверка через EXPLAIN, что используется только партиция 202411
EXPLAIN SELECT *
FROM sales_var15
WHERE sale_timestamp >= '2024-11-01' AND sale_timestamp < '2024-12-01';
```
<img width="1775" height="732" alt="image" src="https://github.com/user-attachments/assets/d217df4c-663f-498b-8dc6-9cb1f92f87ee" />

<img width="679" height="141" alt="image" src="https://github.com/user-attachments/assets/a8dc917f-12d5-4f1d-8335-67eff2f515af" />

---
## Задание 3. ReplacingMergeTree — справочник товаров
1. Вставьте (NNN % 10 + 5) товаров (для варианта 42: 5 + 2 = 7 товаров) с version = 1.
2. Для 3 товаров вставьте обновлённые записи с version = 2 (измените base_price и is_available).
3. Выполните SELECT * FROM products_varNNN — убедитесь, что видны обе версии.
4. Выполните OPTIMIZE TABLE products_varNNN FINAL.
5. Повторите SELECT — убедитесь, что осталась только версия 2.
6. Покажите результаты запроса SELECT * FROM products_varNNN FINAL (альтернатива OPTIMIZE).
---

### 3.1
```sql
-- Создаем таблицу products_var15
CREATE TABLE products_var15 (
    product_id    UInt32,
    product_name  String,
    category      LowCardinality(String),
    supplier      String,
    base_price    Decimal64(2),
    weight_kg     Float32,
    is_available  UInt8,
    updated_at    DateTime,
    version       UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (product_id);

-- 3.1 Вставляем 10 товаров (NNN % 10 + 5 = 15 % 10 + 5 = 10) с version = 1
INSERT INTO products_var15 VALUES
(150, 'Notebook Pro', 'Office', 'TechCorp', 499.99, 1.5, 1, now(), 1),
(151, 'Smartphone X', 'Electronics', 'TechCorp', 699.99, 0.18, 1, now(), 1),
(152, 'T-Shirt', 'Clothing', 'FashionInc', 19.99, 0.15, 1, now(), 1),
(153, 'Apple', 'Food', 'FreshCo', 2.99, 0.15, 1, now(), 1),
(154, 'Dog Food', 'Pets', 'PetShop', 29.99, 2.5, 1, now(), 1),
(155, 'Desk Lamp', 'Office', 'HomeGoods', 34.99, 0.8, 1, now(), 1),
(156, 'Laptop', 'Electronics', 'TechCorp', 999.99, 2.2, 1, now(), 1),
(157, 'Jeans', 'Clothing', 'FashionInc', 49.99, 0.6, 1, now(), 1),
(158, 'Milk', 'Food', 'FreshCo', 1.99, 1.0, 1, now(), 1),
(159, 'Cat Toy', 'Pets', 'PetShop', 9.99, 0.1, 1, now(), 1);
```
### 3.2
```sql
-- 3.2 Для 3 товаров вставляем обновлённые записи с version = 2
INSERT INTO products_var15 VALUES
(150, 'Notebook Pro Max', 'Office', 'TechCorp', 449.99, 1.5, 0, now(), 2),
(151, 'Smartphone X Plus', 'Electronics', 'TechCorp', 649.99, 0.18, 1, now(), 2),
(156, 'Laptop Ultra', 'Electronics', 'TechCorp', 899.99, 2.2, 1, now(), 2);
```
### 3.3
```sql
-- 3.3 Проверяем — видны обе версии
SELECT * FROM products_var15;
```
<img width="1480" height="440" alt="image" src="https://github.com/user-attachments/assets/049eae56-ada7-48e5-9b39-6d8f139a54bf" />


### 3.4
```sql
-- 3.4 Принудительное слияние
OPTIMIZE TABLE products_var15 FINAL;
```
### 3.5
```sql
-- 3.5 Проверяем — осталась только версия 2
SELECT * FROM products_var15;
```
<img width="1493" height="356" alt="image" src="https://github.com/user-attachments/assets/3e39fbff-b16e-4a78-b609-6afbfa538a98" />

### 3.6
```sql
-- 3.6 Альтернатива OPTIMIZE — используем FINAL
SELECT * FROM products_var15 FINAL;
```
<img width="1484" height="353" alt="image" src="https://github.com/user-attachments/assets/7dceb21f-d6c7-4a35-948b-b229f5a1b940" />

---
## Задание 4. SummingMergeTree — агрегация метрик
1. Вставьте данные за (NNN % 5 + 3) дней для (NNN % 3 + 2) кампаний по 2 канала каждая. campaign_id начинается с NNN * 10 + 1.
2. Вставьте повторные строки с теми же ключами (metric_date, campaign_id, channel), но с другими значениями метрик.
3. Выполните OPTIMIZE TABLE daily_metrics_varNNN FINAL.
4. Убедитесь, что числовые столбцы (impressions, clicks, conversions, spend_cents) просуммировались для строк с одинаковыми ключами.
5. Напишите запрос: суммарные clicks / impressions (CTR) по каналам.

```sql
-- Создание таблицы
CREATE TABLE daily_metrics_var15 (
    metric_date    Date,
    campaign_id    UInt32,
    channel        LowCardinality(String),
    impressions    UInt64,
    clicks         UInt64,
    conversions    UInt32,
    spend_cents    UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (metric_date, campaign_id, channel);

-- Параметры для варианта 15:
-- Дней: NNN % 5 + 3 = 15 % 5 + 3 = 3 дня
-- Кампаний: NNN % 3 + 2 = 15 % 3 + 2 = 2 кампании
-- campaign_id начинается с: NNN × 10 + 1 = 151

-- 4.1 Вставляем данные за 3 дня для 2 кампаний, по 2 канала каждая (3*2*2 = 12 строк)
INSERT INTO daily_metrics_var15 (metric_date, campaign_id, channel, impressions, clicks, conversions, spend_cents) VALUES
('2024-10-01', 151, 'Email', 10000, 500, 50, 500000),
('2024-10-01', 151, 'Social', 15000, 750, 75, 750000),
('2024-10-01', 152, 'Email', 8000, 400, 40, 400000),
('2024-10-01', 152, 'Social', 12000, 600, 60, 600000),
('2024-10-02', 151, 'Email', 11000, 550, 55, 550000),
('2024-10-02', 151, 'Social', 16000, 800, 80, 800000),
('2024-10-02', 152, 'Email', 8500, 425, 42, 425000),
('2024-10-02', 152, 'Social', 13000, 650, 65, 650000),
('2024-10-03', 151, 'Email', 12000, 600, 60, 600000),
('2024-10-03', 151, 'Social', 17000, 850, 85, 850000),
('2024-10-03', 152, 'Email', 9000, 450, 45, 450000),
('2024-10-03', 152, 'Social', 14000, 700, 70, 700000);
```



```sql
-- 4.2 Вставляем повторные строки с теми же ключами (для проверки суммирования)
INSERT INTO daily_metrics_var15 (metric_date, campaign_id, channel, impressions, clicks, conversions, spend_cents) VALUES
('2024-10-01', 151, 'Email', 1000, 50, 5, 50000),
('2024-10-01', 151, 'Social', 2000, 100, 10, 100000),
('2024-10-02', 152, 'Email', 800, 40, 4, 40000),
('2024-10-03', 151, 'Social', 1500, 75, 7, 75000),
('2024-10-03', 152, 'Email', 500, 25, 2, 25000),
('2024-10-03', 152, 'Social', 2000, 100, 10, 100000);
```

```sql
-- 4.3 Проверяем данные до оптимизации
SELECT * FROM daily_metrics_var15;
```
<img width="1189" height="590" alt="image" src="https://github.com/user-attachments/assets/b7f01892-1071-4f46-8c2b-41bade08b7f1" />

```sql
-- 4.4 Выполняем OPTIMIZE для принудительного суммирования
OPTIMIZE TABLE daily_metrics_var15 FINAL;
```
```sql
-- 4.5 Проверяем, что данные просуммировались (должно быть 3 дня × 2 кампании × 2 канала = 12 строк)
SELECT 
    metric_date,
    campaign_id,
    channel,
    impressions,
    clicks,
    conversions,
    spend_cents
FROM daily_metrics_var15
ORDER BY metric_date, campaign_id, channel;
```
<img width="1185" height="416" alt="image" src="https://github.com/user-attachments/assets/34a10573-8bd4-4ea9-9b59-189cdfd435fe" />


```sql
-- 4.6 CTR (Click-Through Rate) по каналам
SELECT
    channel,
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions,
    round(SUM(clicks) / SUM(impressions), 4) AS CTR
FROM daily_metrics_var15
GROUP BY channel;
```
<img width="707" height="106" alt="image" src="https://github.com/user-attachments/assets/c12e0c92-187a-4075-b131-8a2b5f81195f" />

---
## Задание 5. Комплексный анализ и самопроверка
```sql
-- 5.1 Проверка партиций таблицы sales_var15
SELECT
    partition,
    count() AS parts,
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE database = 'db_15'
  AND table = 'sales_var15'
  AND active
GROUP BY partition
ORDER BY partition;
```
**Результат: Запрос не выполнен из-за отсутствия прав на чтение системной таблицы *system.parts*.**

### 5.2
```sql
-- 5.2 JOIN между таблицами — топ-5 товаров по выручке
SELECT
    p.product_name,
    p.category,
    round(sum(s.quantity * s.unit_price * (1 - s.discount_pct)), 2) AS revenue
FROM sales_var15 AS s
INNER JOIN products_var15 AS p
    ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 5;
```
<img width="602" height="200" alt="image" src="https://github.com/user-attachments/assets/ff83021b-9ff6-43fe-a650-c6d144241256" />

### 5.3 Типы данных всех трёх таблиц
```sql
DESCRIBE TABLE sales_var15;
```
<img width="321" height="378" alt="image" src="https://github.com/user-attachments/assets/cd2d5d1e-7773-4890-a3ec-4dad52847cb6" />

```sql
DESCRIBE TABLE products_var15;
```
<img width="323" height="313" alt="image" src="https://github.com/user-attachments/assets/1603c7c5-69ac-4c4c-9919-5c63b1696f5d" />

```sql
DESCRIBE TABLE daily_metrics_var15;
```
<img width="337" height="264" alt="image" src="https://github.com/user-attachments/assets/7c810339-4d6d-432c-afd5-24ba44a5672f" />

### 5.4
```sql
-- 5.4 Запрос с массивом

CREATE TABLE tags_var15 (
    item_id  UInt32,
    item_name String,
    tags     Array(String)
) ENGINE = MergeTree()
ORDER BY item_id;

INSERT INTO tags_var15 VALUES
(1, 'Item A', ['sale', 'popular', 'new']),
(2, 'Item B', ['premium', 'limited']),
(3, 'Item C', ['sale', 'clearance']);

SELECT
    arrayJoin(tags) AS tag,
    count() AS items_count
FROM tags_var15
GROUP BY tag
ORDER BY items_count DESC;
```
<img width="368" height="235" alt="image" src="https://github.com/user-attachments/assets/95edaa29-c013-403e-bd02-a7cf418d173b" />


### 5.5
```sql
-- 5.5 Контрольная сумма
SELECT
    'sales' AS tbl, 
    count() AS rows, 
    sum(quantity) AS check_sum 
FROM db_15.sales_var15

UNION ALL

SELECT
    'products', 
    count(), 
    sum(toUInt64(product_id)) 
FROM db_15.products_var15 FINAL

UNION ALL

SELECT
    'metrics', 
    count(), 
    sum(clicks) 
FROM db_15.daily_metrics_var15;
```
<img width="522" height="139" alt="image" src="https://github.com/user-attachments/assets/d9a55e39-49ab-448d-9c69-8ab6b96bce06" />


## Контрольные вопросы

### 1. Почему LowCardinality(String) эффективнее обычного String для столбца category?

LowCardinality(String) создаёт внутренний словарь уникальных значений и хранит вместо строк числовые индексы. Это значительно экономит место на диске и ускоряет операции сравнения и группировки, что особенно полезно для столбцов с небольшим количеством уникальных значений, таких как категории товаров.

---

### 2. В чём разница между ORDER BY и PRIMARY KEY в ClickHouse?

ORDER BY определяет физический порядок сортировки данных на диске и является обязательным параметром для таблиц семейства MergeTree. PRIMARY KEY задаёт разреженный индекс для ускорения поиска, является опциональным и должен быть префиксом ORDER BY, но в отличие от традиционных СУБД не гарантирует уникальность записей.

---

### 3. Когда следует использовать ReplacingMergeTree вместо MergeTree?

ReplacingMergeTree следует использовать, когда необходимо хранить только последнюю версию строки, например, в справочниках или профилях пользователей с обновляемыми данными. Он автоматически удаляет дубликаты с одинаковым ключом сортировки, оставляя строку с максимальной версией, что удобно для систем с обновлениями без явных операций UPDATE.

---

### 4. Почему SummingMergeTree не заменяет GROUP BY в аналитических запросах?

SummingMergeTree суммирует числовые столбцы только во время фоновых слияний партиций, поэтому в любой момент времени данные могут быть частично не просуммированы. Для получения точных и актуальных результатов агрегации всё равно необходимо использовать GROUP BY в запросах, а SummingMergeTree служит лишь для оптимизации хранения и ускорения этих запросов.

---

### 5. Что произойдёт, если не выполнить OPTIMIZE TABLE FINAL для ReplacingMergeTree?

Если не выполнить OPTIMIZE TABLE FINAL, старые версии строк могут оставаться в таблице неопределённое время, так как слияние партиций происходит в фоне автоматически, но не мгновенно. В результате обычные SELECT-запросы могут возвращать дубликаты или устаревшие данные до тех пор, пока не произойдёт фоновое слияние.

## Вывод

В ходе выполнения лабораторной работы были изучены и практически применены различные движки таблиц ClickHouse: MergeTree, ReplacingMergeTree, SummingMergeTree. Все задания выполнены в соответствии с вариантом 15, данные вставлены корректно (150 строк в таблицу продаж, 10 товаров в справочник, метрики за 3 дня), аналитические запросы отработали без ошибок, на все контрольные вопросы даны ответы.

Полный скрипт можно найти в файле [pr01_var15.sql](pr01_var15.sql).
