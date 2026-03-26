1. Запуск системы

1.1 Поднять PostgreSQL
docker compose down -v
docker compose up -d
1.2 Проверить, что база доступна
docker exec -it shop-postgres psql -U postgres -d shopdb -c "SELECT 1;"
1.3 Запустить приложение
mvn spring-boot:run
1.4 Проверить таблицы
docker exec -it shop-postgres psql -U postgres -d shopdb -c "\dt"

2. E2E сценарий через curl

2.1 Регистрация SELLER
curl -s -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "seller@test.com",
    "password": "password123",
    "name": "Test Seller",
    "role": "SELLER"
}'
SELLER_TOKEN="сюда_accessToken"

2.2 Регистрация BUYER
curl -s -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "buyer@test.com",
    "password": "password123",
    "name": "Test Buyer",
    "role": "BUYER"
}'
BUYER_TOKEN=

2.3 Создать первый товар
curl -s -X POST http://localhost:8080/api/v1/products \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $SELLER_TOKEN" \
  -d '{
    "name": "Laptop",
    "description": "Gaming laptop",
    "price": 1500.00,
    "stock": 10,
    "category": "Electronics",
    "status": "ACTIVE"
}'
PRODUCT1="сюда_id"

2.4 Создать второй товар
curl -s -X POST http://localhost:8080/api/v1/products \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $SELLER_TOKEN" \
  -d '{
    "name": "Mouse",
    "description": "Wireless mouse",
    "price": 25.00,
    "stock": 100,
    "category": "Electronics",
    "status": "ACTIVE"
}'
PRODUCT2="сюда_id"

2.5 Получить список активных товаров
curl -s "http://localhost:8080/api/v1/products?page=0&size=10&status=ACTIVE"

2.6 Создать заказ BUYER
curl -s -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $BUYER_TOKEN" \
  -d "{
    \"items\": [
      {\"productId\": \"$PRODUCT1\", \"quantity\": 2},
      {\"productId\": \"$PRODUCT2\", \"quantity\": 3}
    ]
  }"
ORDER_ID="сюда_id"

2.7 Отменить заказ
curl -s -X POST "http://localhost:8080/api/v1/orders/$ORDER_ID/cancel" \
  -H "Authorization: Bearer $BUYER_TOKEN"

Подождать 60 секунд!!!!

2.8 Создать заказ с промокодом
curl -s -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $BUYER_TOKEN" \
  -d "{
    \"items\": [
      {\"productId\": \"$PRODUCT1\", \"quantity\": 1}
    ],
    \"promoCode\": \"SAVE10\"
  }"

3. (Если токены истекли ps они истекают через 15 минут, если отвлечься от процесса)

3.1 Логин SELLER
curl -s -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "seller@test.com",
    "password": "password123"
}'
SELLER_TOKEN=

3.2 Логин BUYER
curl -s -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "buyer@test.com",
    "password": "password123"
}'
BUYER_TOKEN=

4. Негативные сценарии

4.1 Валидация невалидного товара
curl -s -X POST http://localhost:8080/api/v1/products \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $SELLER_TOKEN" \
  -d '{"name":"","price":-5,"stock":-1,"category":"","status":"ACTIVE"}'

4.2 SELLER пытается создать заказ
curl -s -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $SELLER_TOKEN" \
  -d "{
    \"items\": [
      {\"productId\": \"$PRODUCT1\", \"quantity\": 1}
    ]
  }"

4.3 Soft delete товара
curl -s -X DELETE "http://localhost:8080/api/v1/products/$PRODUCT2" \
  -H "Authorization: Bearer $SELLER_TOKEN"
Проверка в БД:
docker exec -it shop-postgres psql -U postgres -d shopdb -c "SELECT id, name, price, stock, status FROM products;"
Проверка списка активных товаров:
curl -s "http://localhost:8080/api/v1/products?page=0&size=10&status=ACTIVE"

5. Проверка данных в БД через SELECT

5.1 Войти в psql
docker exec -it shop-postgres psql -U postgres -d shopdb

5.2 Выполнить запросы
SELECT id, email, name, role FROM users;
SELECT id, name, price, stock, status FROM products;
SELECT id, user_id, status, total_price, discounted_price FROM orders;
SELECT id, order_id, product_name, quantity, price_at_order FROM order_items;
SELECT id, code, discount_percent, current_uses FROM promo_codes;
SELECT id, user_id, operation_type, created_at FROM user_operations;

5.3 Выйти из psql
\q

6. Лень печатать 

SELLER_TOKEN="..."
BUYER_TOKEN="..."
PRODUCT1="..."
PRODUCT2="..."
ORDER_ID="..."