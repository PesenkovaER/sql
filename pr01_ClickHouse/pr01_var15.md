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

## Задание 2. Аналитические запросы
**Общая выручка по категориям**
```sql
SELECT
    category,
    round(SUM(quantity * unit_price * (1 - discount_pct)), 2) AS total_revenue
FROM sales_var15
GROUP BY category
ORDER BY total_revenue DESC;
```
<img width="415" height="208" alt="image" src="https://github.com/user-attachments/assets/02f4f976-8e6e-49eb-8fe4-8f86e6db4b08" />

**Топ-3 клиента по количеству покупок**
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

**Средний чек по месяцам**
```sql
SELECT
    toYYYYMM(sale_timestamp) AS month,
    round(AVG(quantity * unit_price), 2) AS avg_check
FROM sales_var15
GROUP BY month
ORDER BY month;
```
<img width="391" height="134" alt="image" src="https://github.com/user-attachments/assets/d255da37-124b-4882-96af-d4129c09401b" />

**Фильтрация по партиции**
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

<img width="518" height="139" alt="image" src="https://github.com/user-attachments/assets/c6b179a3-8555-4fa3-982d-9d04e7e80750" />

--
## Задание 3. ReplacingMergeTree — справочник товаров
1. Вставьте (NNN % 10 + 5) товаров (для варианта 42: 5 + 2 = 7 товаров) с version = 1.
2. Для 3 товаров вставьте обновлённые записи с version = 2 (измените base_price и is_available).
3. Выполните SELECT * FROM products_varNNN — убедитесь, что видны обе версии.
4. Выполните OPTIMIZE TABLE products_varNNN FINAL.
5. Повторите SELECT — убедитесь, что осталась только версия 2.
6. Покажите результаты запроса SELECT * FROM products_varNNN FINAL (альтернатива OPTIMIZE).

*3.1*
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
*3.2*
```sql
-- 3.2 Для 3 товаров вставляем обновлённые записи с version = 2
INSERT INTO products_var15 VALUES
(150, 'Notebook Pro Max', 'Office', 'TechCorp', 449.99, 1.5, 0, now(), 2),
(151, 'Smartphone X Plus', 'Electronics', 'TechCorp', 649.99, 0.18, 1, now(), 2),
(156, 'Laptop Ultra', 'Electronics', 'TechCorp', 899.99, 2.2, 1, now(), 2);
```
*3.3*
```sql
-- 3.3 Проверяем — видны обе версии
SELECT * FROM products_var15;
```
<img width="1480" height="440" alt="image" src="https://github.com/user-attachments/assets/049eae56-ada7-48e5-9b39-6d8f139a54bf" />

*3.4*
```sql
-- 3.4 Принудительное слияние
OPTIMIZE TABLE products_var15 FINAL;
```
*3.5*
```sql
-- 3.5 Проверяем — осталась только версия 2
SELECT * FROM products_var15;
```
<img width="1493" height="356" alt="image" src="https://github.com/user-attachments/assets/3e39fbff-b16e-4a78-b609-6afbfa538a98" />

```sql
-- 3.6 Альтернатива OPTIMIZE — используем FINAL
SELECT * FROM products_var15 FINAL;
```
<img width="1484" height="353" alt="image" src="https://github.com/user-attachments/assets/7dceb21f-d6c7-4a35-948b-b229f5a1b940" />


<img width="1185" height="403" alt="image" src="https://github.com/user-attachments/assets/d463a7c0-23de-49af-8518-abdefa74f48c" />


```sql

```

<img width="1189" height="590" alt="image" src="https://github.com/user-attachments/assets/b7f01892-1071-4f46-8c2b-41bade08b7f1" />

```sql

```
<img width="1185" height="416" alt="image" src="https://github.com/user-attachments/assets/34a10573-8bd4-4ea9-9b59-189cdfd435fe" />

<img width="707" height="106" alt="image" src="https://github.com/user-attachments/assets/c12e0c92-187a-4075-b131-8a2b5f81195f" />


5.2
```sql

```
<img width="602" height="200" alt="image" src="https://github.com/user-attachments/assets/ff83021b-9ff6-43fe-a650-c6d144241256" />

5.3 типы данных
```sql

```
<img width="321" height="378" alt="image" src="https://github.com/user-attachments/assets/cd2d5d1e-7773-4890-a3ec-4dad52847cb6" />

<img width="323" height="313" alt="image" src="https://github.com/user-attachments/assets/1603c7c5-69ac-4c4c-9919-5c63b1696f5d" />

<img width="337" height="264" alt="image" src="https://github.com/user-attachments/assets/7c810339-4d6d-432c-afd5-24ba44a5672f" />

5.4
```sql

```
<img width="368" height="235" alt="image" src="https://github.com/user-attachments/assets/95edaa29-c013-403e-bd02-a7cf418d173b" />


5.5
```sql

```
<img width="522" height="139" alt="image" src="https://github.com/user-attachments/assets/d9a55e39-49ab-448d-9c69-8ab6b96bce06" />


