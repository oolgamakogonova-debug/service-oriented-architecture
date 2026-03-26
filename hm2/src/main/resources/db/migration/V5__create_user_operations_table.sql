CREATE TABLE user_operations (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        UUID         NOT NULL REFERENCES users(id),
    operation_type VARCHAR(50)  NOT NULL,
    order_id       UUID         REFERENCES orders(id),
    details        TEXT,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_user_ops_user ON user_operations(user_id);
CREATE INDEX idx_user_ops_user_type ON user_operations(user_id, operation_type);
CREATE INDEX idx_user_ops_user_created ON user_operations(user_id, created_at);