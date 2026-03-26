CREATE TABLE promo_codes (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code             VARCHAR(100) NOT NULL UNIQUE,
    discount_percent DECIMAL(5, 2) NOT NULL,
    active           BOOLEAN      NOT NULL DEFAULT TRUE,
    max_uses         INTEGER,
    current_uses     INTEGER      NOT NULL DEFAULT 0,
    min_order_amount DECIMAL(12, 2),
    expires_at       TIMESTAMP WITH TIME ZONE,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_promo_codes_code ON promo_codes(code);

INSERT INTO promo_codes (code, discount_percent, active, max_uses, current_uses, min_order_amount)
VALUES ('SAVE10', 10.00, TRUE, 100, 0, 50.00);

INSERT INTO promo_codes (code, discount_percent, active, max_uses, current_uses, min_order_amount)
VALUES ('SAVE20', 20.00, TRUE, 50, 0, 100.00);