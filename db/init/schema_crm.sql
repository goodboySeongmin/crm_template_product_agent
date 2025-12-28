-- ============================================================
-- AMORE ERD_v2 tables into existing crm database
-- - Do NOT create users table (already exists in crm)
-- - FK users(user_id) will reference existing crm.users
-- - MySQL 8 / InnoDB / utf8mb4
-- ============================================================

USE crm;

-- ------------------------------------------------------------
-- 1) products
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
  prod_sn BIGINT NOT NULL,
  online_prod_code VARCHAR(64) NULL,
  brand VARCHAR(64) NULL,
  product_name VARCHAR(255) NULL,
  detail_url VARCHAR(1024) NULL,
  image_url_main VARCHAR(1024) NULL,

  price_original INT NULL,
  price_sale INT NULL,
  discount_rate FLOAT NULL,
  promo_text VARCHAR(255) NULL,

  description_img_urls TEXT NULL,

  category_paths_all TEXT NULL,
  product_gender_target VARCHAR(32) NULL,
  concern_tags_all TEXT NULL,

  collected_at DATETIME NULL,

  PRIMARY KEY (prod_sn),
  KEY idx_products_brand (brand),
  KEY idx_products_name (product_name),
  KEY idx_products_collected_at (collected_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ------------------------------------------------------------
-- 2) product_category_map
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_category_map (
  id BIGINT NOT NULL AUTO_INCREMENT,
  prod_sn BIGINT NOT NULL,

  category_depth1 VARCHAR(64) NULL,
  category_depth2 VARCHAR(64) NULL,
  category_depth3 VARCHAR(64) NULL,
  category_path VARCHAR(255) NOT NULL,

  collected_at DATETIME NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uq_pcm_prod_path (prod_sn, category_path),
  KEY idx_pcm_prod_sn (prod_sn),
  KEY idx_pcm_collected_at (collected_at),

  CONSTRAINT fk_pcm_prod
    FOREIGN KEY (prod_sn) REFERENCES products(prod_sn)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ------------------------------------------------------------
-- 3) product_concern_map
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_concern_map (
  id BIGINT NOT NULL AUTO_INCREMENT,
  prod_sn BIGINT NOT NULL,
  product_concern VARCHAR(64) NOT NULL,
  collected_at DATETIME NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uq_pcon_prod_concern (prod_sn, product_concern),
  KEY idx_pcon_prod_sn (prod_sn),
  KEY idx_pcon_collected_at (collected_at),

  CONSTRAINT fk_pcon_prod
    FOREIGN KEY (prod_sn) REFERENCES products(prod_sn)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ------------------------------------------------------------
-- 4) product_ocr_text
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_ocr_text (
  id BIGINT NOT NULL AUTO_INCREMENT,
  prod_sn BIGINT NOT NULL,
  image_seq INT NOT NULL,

  raw_text TEXT NULL,
  clean_text TEXT NULL,
  source_image_url VARCHAR(1024) NOT NULL,

  collected_at DATETIME NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uq_pocr_prod_seq (prod_sn, image_seq),
  KEY idx_pocr_prod_sn (prod_sn),
  KEY idx_pocr_collected_at (collected_at),

  CONSTRAINT fk_pocr_prod
    FOREIGN KEY (prod_sn) REFERENCES products(prod_sn)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ------------------------------------------------------------
-- 5) product_ocr_tag_map
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_ocr_tag_map (
  id BIGINT NOT NULL AUTO_INCREMENT,
  prod_sn BIGINT NOT NULL,

  tag_type VARCHAR(32) NOT NULL,
  tag_value VARCHAR(128) NOT NULL,
  confidence FLOAT NULL,

  collected_at DATETIME NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uq_pot_prod_type_value (prod_sn, tag_type, tag_value),
  KEY idx_pot_type_value (tag_type, tag_value),
  KEY idx_pot_prod_sn (prod_sn),
  KEY idx_pot_collected_at (collected_at),

  CONSTRAINT fk_pot_prod
    FOREIGN KEY (prod_sn) REFERENCES products(prod_sn)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ------------------------------------------------------------
-- 6) carts
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS carts (
  cart_id BIGINT NOT NULL AUTO_INCREMENT,
  user_id VARCHAR(64) NOT NULL,

  status VARCHAR(32) NOT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,

  PRIMARY KEY (cart_id),
  KEY idx_carts_user_status (user_id, status),
  KEY idx_carts_updated_at (updated_at),

  CONSTRAINT fk_carts_user
    FOREIGN KEY (user_id) REFERENCES users(user_id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ------------------------------------------------------------
-- 7) cart_items
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cart_items (
  cart_item_id BIGINT NOT NULL AUTO_INCREMENT,
  cart_id BIGINT NOT NULL,
  prod_sn BIGINT NOT NULL,

  quantity INT NOT NULL,
  unit_price INT NULL,
  added_at DATETIME NULL,

  PRIMARY KEY (cart_item_id),
  KEY idx_cart_items_cart (cart_id),
  KEY idx_cart_items_prod (prod_sn),

  CONSTRAINT fk_ci_cart
    FOREIGN KEY (cart_id) REFERENCES carts(cart_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_ci_prod
    FOREIGN KEY (prod_sn) REFERENCES products(prod_sn)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ------------------------------------------------------------
-- 8) orders
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT NOT NULL AUTO_INCREMENT,
  user_id VARCHAR(64) NOT NULL,

  order_status VARCHAR(32) NOT NULL,
  total_amount INT NULL,
  ordered_at DATETIME NULL,

  PRIMARY KEY (order_id),
  KEY idx_orders_user_ordered (user_id, ordered_at),
  KEY idx_orders_status (order_status),

  CONSTRAINT fk_orders_user
    FOREIGN KEY (user_id) REFERENCES users(user_id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ------------------------------------------------------------
-- 9) order_items
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_items (
  order_item_id BIGINT NOT NULL AUTO_INCREMENT,
  order_id BIGINT NOT NULL,
  prod_sn BIGINT NOT NULL,

  quantity INT NOT NULL,
  unit_price INT NULL,
  created_at DATETIME NULL,

  PRIMARY KEY (order_item_id),
  KEY idx_oi_order (order_id),
  KEY idx_oi_prod (prod_sn),

  CONSTRAINT fk_oi_order
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_oi_prod
    FOREIGN KEY (prod_sn) REFERENCES products(prod_sn)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
