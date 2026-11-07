CREATE TABLE flash_sales (
    id uuid PRIMARY KEY,
    stock bigint NOT NULL CHECK (stock >= 0)
);
CREATE TABLE orders (
    id uuid PRIMARY KEY,
    flash_sale_id uuid NOT NULL,
    user_id uuid NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (flash_sale_id, user_id)
);
INSERT INTO flash_sales (id, stock)
VALUES ('00000000-0000-7000-8000-000000000000', 10000000000);