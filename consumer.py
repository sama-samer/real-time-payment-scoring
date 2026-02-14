
import json
import pandas as pd
import numpy as np
import mysql.connector  
from kafka import KafkaConsumer 
from model import rf_cal
import logging
from datetime import datetime

logging.basicConfig(
    filename='payment_scoring.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

KAFKA_TOPIC = 'transactions'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9094'
MYSQL_HOST = 'localhost'
MYSQL_USER = 'app_user'
MYSQL_PASSWORD = 'app_password'
MYSQL_DATABASE = 'payments_db'


def create_features(tx):
    amount = tx.get('amount', 0)
    customer_age = tx.get('customer_age', 30)
    status = tx.get('status', '')
    trans_type = tx.get('transaction_type', '')
    currency = tx.get('currency', '')
    
    hour = tx.get('transaction_hour', datetime.now().hour)
    day_of_week = tx.get('day_of_week', tx.get('transaction_day_of_week', datetime.now().isoweekday()))
    
    # Time-based features
    is_night_time = 1 if (hour >= 22 or hour < 6) else 0
    is_business_hours = 1 if (9 <= hour <= 17) else 0
    is_weekend = 1 if (day_of_week >= 6) else 0
    
    normalized_amount = np.log1p(amount) / 10
    
    feature_dict = {}
    
    feature_dict['V1'] = normalized_amount * (-1 if status != "completed" else 1)
    feature_dict['V2'] = normalized_amount * (customer_age / 50)
    feature_dict['V3'] = -normalized_amount if trans_type == "online" else normalized_amount
    feature_dict['V4'] = normalized_amount if currency == "USD" else -normalized_amount * 2
    feature_dict['V5'] = (amount / 10000) * (1 if status == "completed" else -2)
    
    feature_dict['V6'] = customer_age / 100 * (1 + is_night_time)
    feature_dict['V7'] = -customer_age / 100 if status == "failed" else customer_age / 100
    feature_dict['V8'] = amount / 50000 * (1 + is_weekend)
    feature_dict['V9'] = -amount / 50000 if trans_type != "online" else amount / 50000
    feature_dict['V10'] = (customer_age - 35) / 20 * (1 if is_business_hours else -1)
    
    feature_dict['V11'] = np.sqrt(amount) / 100 * (1 + is_night_time * 2)
    feature_dict['V12'] = -np.sqrt(amount) / 100 if currency != "USD" else np.sqrt(amount) / 100
    feature_dict['V13'] = (amount * customer_age) / 100000 * (1 + is_weekend)
    feature_dict['V14'] = -(amount * customer_age) / 100000 if status != "completed" else (amount * customer_age) / 100000
    feature_dict['V15'] = (amount / customer_age if customer_age > 0 else 0) * (1 + is_night_time)
    
    feature_dict['V16'] = hour / 24 * normalized_amount
    feature_dict['V17'] = np.log1p(amount) * np.log1p(customer_age) * (1 if is_business_hours else -1)
    feature_dict['V18'] = -np.log1p(amount) * np.log1p(customer_age) if status == "failed" else np.log1p(amount) * np.log1p(customer_age)
    feature_dict['V19'] = (amount / 1000) * is_night_time * is_weekend
    feature_dict['V20'] = day_of_week / 7 * normalized_amount
    
    for i in range(21, 29):
        base_value = (amount / (i * 10000)) * (1 if i % 2 == 0 else -1)
        time_modifier = 1 + (is_night_time * 0.5) + (is_weekend * 0.3)
        feature_dict[f'V{i}'] = base_value * time_modifier
    
    feature_dict['Amount'] = amount
    feature_dict['_log_amount'] = np.log1p(amount)
    feature_dict['Hour_from_start_mod24'] = hour
    feature_dict['is_night_proxy'] = is_night_time
    feature_dict['is_business_hours_proxy'] = is_business_hours
    
    return pd.DataFrame([feature_dict])


def get_risk_level(fraud_score):
    if fraud_score > 0.15:
        return "High"
    elif fraud_score > 0.05:
        return "Medium"
    else:
        return "Low"


def save_transaction(cursor, tx, fraud_score, risk_level):
    """Save scored transaction to database"""
    hour = tx.get('transaction_hour', datetime.now().hour)
    day_of_week = tx.get('day_of_week', tx.get('transaction_day_of_week', datetime.now().isoweekday()))
    
    is_night_time = 1 if (hour >= 22 or hour < 6) else 0
    is_business_hours = 1 if (9 <= hour <= 17) else 0
    is_weekend = 1 if (day_of_week >= 6) else 0
    
    cursor.execute("""
        INSERT INTO transactions 
        (amount, currency, transaction_type, status, 
         transaction_hour, transaction_day_of_week, is_night_time, is_business_hours, is_weekend,
         customer_age, fraud_score, risk_level)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        tx.get('amount', 0),
        tx.get('currency', ''),
        tx.get('transaction_type', ''),
        tx.get('status', ''),
        hour,
        day_of_week,
        is_night_time,
        is_business_hours,
        is_weekend,
        tx.get('customer_age', 30),
        fraud_score,
        risk_level
    ))


def main():
    
    db = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = db.cursor()
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    try:
        for message in consumer:
            tx = message.value
            
            try:
                features = create_features(tx)
                fraud_score = rf_cal.predict_proba(features)[0][1]
                risk_level = get_risk_level(fraud_score)
                
                save_transaction(cursor, tx, fraud_score, risk_level)
                db.commit()
                
                print(f" ${tx.get('amount', 0):.2f} - Risk: {risk_level} ({fraud_score:.4f})")
                logging.info(f"Processed: Amount=${tx.get('amount', 0)}, Score={fraud_score:.4f}, Risk={risk_level}")
                
            except Exception as e:
                print(f"✗ Error: {e}")
                logging.error(f"Processing error: {e}")
                db.rollback()
                
    except KeyboardInterrupt:
        print("\n Shutting down")
    finally:
        consumer.close()
        cursor.close()
        db.close()


if __name__ == "__main__":
    main()