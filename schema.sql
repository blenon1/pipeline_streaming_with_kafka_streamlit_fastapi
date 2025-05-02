CREATE DATABASE data_warehouse;
USE data_warehouse;

CREATE TABLE users (
    user_id VARCHAR(20) PRIMARY KEY,
    user_name VARCHAR(100)
);

CREATE TABLE transactions (
    transaction_id VARCHAR(30) PRIMARY KEY,
    timestamp DATETIME,
    user_id VARCHAR(20),
    product_id VARCHAR(20),
    amount DECIMAL(10,2),
    currency VARCHAR(10),
    transaction_type VARCHAR(20),
    status VARCHAR(20),
    payment_method VARCHAR(50),
    location_city VARCHAR(100),
    location_country VARCHAR(100),
    received_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE shipping_addresses (
    transaction_id VARCHAR(30) PRIMARY KEY,
    street VARCHAR(100),
    zip_code VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
);

CREATE TABLE device_info (
    transaction_id VARCHAR(30) PRIMARY KEY,
    os VARCHAR(50),
    browser VARCHAR(50),
    ip_address VARCHAR(50),
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
);
