# Лабораторная работа №2  
## Использование соединений (JOIN), подзапросов и функций преобразования данных

**Номер варианта:** 15  

---
# Цель работы

Освоить методы объединения таблиц (JOIN, UNION), работу с подзапросами и функции преобразования данных (CASE, COALESCE) в PostgreSQL.

# Задания

## Задание 1. Использование JOIN  
Вывести все продажи (модель, цена), совершенные через интернет (`channel = 'internet'`).

Скриншот выполнения SELECT (JOIN, основная БД)

<img width="628" height="611" alt="image" src="https://github.com/user-attachments/assets/6c2c08b3-272f-4000-8ddb-46fee59145e9" />

---

## Задание 2. Использование UNION ALL  
Объединить (UNION ALL) список `dealership_id` из таблицы `sales` и из таблицы `dealerships`.

Скриншот выполнения SELECT (UNION ALL, основная БД)

<img width="381" height="608" alt="image" src="https://github.com/user-attachments/assets/dcdad147-f793-4c96-9119-de2636105dac" />
