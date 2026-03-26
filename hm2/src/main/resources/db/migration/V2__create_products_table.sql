CREATE TABLE products (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255)   NOT NULL,
    description VARCHAR(4000),
    price       DECIMAL(12, 2) NOT NULL,
    stock       INTEGER        NOT NULL DEFAULT 0,
    category    VARCHAR(100)   NOT NULL,
    status      VARCHAR(50)    NOT NULL DEFAULT 'ACTIVE',
    seller_id   UUID           NOT NULL REFERENCES users(id),
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_products_status ON products(status);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_seller ON products(seller_id);