import json
import time
from datetime import datetime
from kafka import KafkaProducer

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9094"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_transactions = [
    {
        "transaction_id": 310,
        "amount": 50.00,
        "currency": "USD",
        "transaction_type": "online",
        "status": "completed",
        "customer_age": 35,
        "transaction_hour": 14,     
        "day_of_week": 3             
    },
    {
        "transaction_id": 320,
        "amount": 15000.00,
        "currency": "USD",
        "transaction_type": "online",
        "status": "completed",
        "customer_age": 22,
        "transaction_hour": 2,        
        "day_of_week": 7              
    },
    {
        "transaction_id": 330,
        "amount": 8500.00,
        "currency": "EUR",
        "transaction_type": "online",
        "status": "failed",
        "customer_age": 45,
        "transaction_hour": 23,      
        "day_of_week": 6              
    },
    {
        "transaction_id": 340,
        "amount": 120.00,
        "currency": "USD",
        "transaction_type": "online",
        "status": "completed",
        "customer_age": 40,
        "transaction_hour": 10,      
        "day_of_week": 2              
    },
    {
        "transaction_id": 350,
        "amount": 99999.00,
        "currency": "USD",
        "transaction_type": "online",
        "status": "completed",
        "customer_age": 18,
        "transaction_hour": 3,        
        "day_of_week": 7              
    },
    {
        "transaction_id": 360,
        "amount": 5.99,
        "currency": "USD",
        "transaction_type": "online",
        "status": "completed",
        "customer_age": 28,
        "transaction_hour": 6,        
        "day_of_week": 1              
    },
    {
        "transaction_id": 370,
        "amount": 50000.00,
        "currency": "GBP",
        "transaction_type": "online",
        "status": "completed",
        "customer_age": 65,
        "transaction_hour": 1,        
        "day_of_week": 5              
    }
]

print("Sending transactions with time patterns...")
print("=" * 80)
print(f"{'ID':<6} {'Amount':>12} {'Hour':>6} {'Day':>4} {'Status':<10} {'Currency'}")
print("=" * 80)

for txn in test_transactions:
    producer.send('transactions', txn)
    
    # Format day of week
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    day_name = days[txn['day_of_week'] - 1]
    
    # Format time
    hour_str = f"{txn['transaction_hour']:02d}:00"
    
    print(f"{txn['transaction_id']:<6} ${txn['amount']:>10,.2f} {hour_str:>6} {day_name:>4} {txn['status']:<10} {txn['currency']}")
    time.sleep(0.3)

producer.flush()
