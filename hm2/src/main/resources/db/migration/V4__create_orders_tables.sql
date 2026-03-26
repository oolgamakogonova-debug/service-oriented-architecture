CREATE TABLE orders (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          UUID           NOT NULL REFERENCES users(id),
    status           VARCHAR(50)    NOT NULL DEFAULT 'CREATED',
    total_price      DECIMAL(12, 2) NOT NULL DEFAULT 0,
    discounted_price DECIMAL(12, 2),
    promo_code_id    UUID           REFERENCES promo_codes(id),
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_user_status ON orders(user_id, status);

CREATE TABLE order_items (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id       UUID           NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id     UUID           NOT NULL REFERENCES products(id),
    product_name   VARCHAR(255)   NOT NULL,
    quantity       INTEGER        NOT NULL,
    price_at_order DECIMAL(12, 2) NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_order_items_order ON order_items(order_id);