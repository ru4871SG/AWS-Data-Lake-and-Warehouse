-- This table should be created in Redshift before executing AWS Glue ETL job to transfer data from S3 to Redshift

CREATE TABLE amazonbestsellers_redshift (
    unique_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    product_num_ratings INT,
    rank INT,
    product_star_rating DOUBLE PRECISION,
    category VARCHAR(255),
    product_photo VARCHAR(1000),
    product_url VARCHAR(1000),
    currency VARCHAR(10),
    short_product_title VARCHAR(500),
    product_title VARCHAR(1000),
    asin VARCHAR(50) NOT NULL,
    fetch_timestamp TIMESTAMP,
    product_price NUMERIC(10,2)
);

COMMIT;