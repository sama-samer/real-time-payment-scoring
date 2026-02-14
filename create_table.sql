-- Active: 1771009200889@@127.0.0.1@3306@payments_db
USE payments_db;

CREATE TABLE transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    amount DECIMAL(8, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    transaction_type VARCHAR(20),
    status VARCHAR(20) NOT NULL,
    -- to calculate the fraud score and save it here
    fraud_score DECIMAL(5, 2),
    -- to translate the score from numbers to words
    risk_level VARCHAR(20),
    transaction_hour INT,
    transaction_day_of_week INT,
    is_night_time BOOLEAN DEFAULT 0,
    is_business_hours BOOLEAN DEFAULT 0,
    is_weekend BOOLEAN DEFAULT 0,
    customer_age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);