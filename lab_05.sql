-- ЗАДАНИЕ 1: Анализ поиска товаров по цене (до создания индекса)
EXPLAIN ANALYZE SELECT * FROM products WHERE base_msrp = 599.99;

-- ЗАДАНИЕ 2: Создание индекса B-Tree для ускорения поиска по цене
CREATE INDEX idx_products_price ON products(base_msrp);
-- Анализ после создания индекса
EXPLAIN ANALYZE SELECT * FROM products WHERE base_msrp = 599.99;

-- ЗАДАНИЕ 3: Оптимизация поиска продаж для конкретного дилера
-- Анализ до индекса (таблица sales)
EXPLAIN ANALYZE SELECT * FROM sales WHERE dealership_id = 5;
-- Создание индекса для поиска по дилеру
CREATE INDEX idx_sales_dealer ON sales(dealership_id);
-- Анализ после индекса
EXPLAIN ANALYZE SELECT * FROM sales WHERE dealership_id = 5;

-- ОЧИСТКА: удаление созданных индексов
DROP INDEX idx_products_price;
DROP INDEX idx_sales_dealer;