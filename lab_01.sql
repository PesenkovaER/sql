-- ЗАДАНИЕ 1.1
SELECT dealership_id, street_address, city, state, date_opened
FROM dealerships
WHERE date_closed IS NULL
ORDER BY date_opened ASC;

-- ЗАДАНИЕ 1.2
SELECT email_id, customer_id, email_subject, sent_date
FROM emails
WHERE (LOWER(email_subject) LIKE '%save%' OR LOWER(email_subject) LIKE '%sale%')
  AND clicked = 't'
ORDER BY sent_date DESC;

-- ЗАДАНИЕ 1.3
-- Создание таблицы vip_clients с клиентами, у которых продажи > 50000
CREATE TABLE vip_clients AS
SELECT DISTINCT c.*
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
WHERE s.sales_amount > 50000;

-- Добавление текстового столбца status
ALTER TABLE vip_clients
ADD COLUMN status text;

-- Обновление: заполнение status значением 'Gold'
UPDATE vip_clients
SET status = 'Gold';

-- Удаление из vip_clients записей, где канал продаж = 'internet'
DELETE FROM vip_clients
WHERE customer_id IN (
    SELECT DISTINCT customer_id
    FROM sales
    WHERE channel = 'internet'
);

-- Проверка
SELECT customer_id, first_name, last_name, email, status
FROM vip_clients;