This project implements a real-time payment scoring system to detect potentially fraudulent transactions and assess credit risk. It uses:
Apache Kafka: for real-time streaming of transactions
Machine Learning Model: a pre-trained Random Forest model (rf_cal) for scoring using the model from hugging face:https://huggingface.co/tarekmasryo/credit-card-fraud-detection?utm_source=chatgpt.com
MySQL: to store transactions with computed fraud scores
Docker: containerizes Kafka, MySQL, and Kafka UI for easy deployment
The system includes both a Kafka producer (to send sample transactions) and a Kafka consumer (to score transactions and store results).

Features
Real-time ingestion of payment transactions
Automatic feature engineering and fraud scoring
Fraud risk classification: Low, Medium, High
Storage in MySQL database
Kafka UI for monitoring topics

Project Structure
.
├── consumer.py         # Kafka consumer: scores transactions and stores in MySQL

├── producer.py         # Kafka producer: sends sample transaction data

├── model.py            # Downloads and loads pre-trained ML model

├── check.py            # Checks features expected by the model 

├── docker-compose.yml  # Docker configuration for Kafka, MySQL, Kafka-UI

├── init-scripts/       # Optional SQL scripts to initialize MySQL schema

├── README.md           # This file

└── requirements.txt    # Python dependencies

Start Docker Services

The docker-compose.yml runs:

Kafka (broker & controller)

Kafka UI (monitor topics at port 8070)

MySQL (database: payments_db, user: app_user)

docker-compose up -d

Check that services are healthy:

docker-compose ps


mysql schema:
<img width="1514" height="375" alt="image" src="https://github.com/user-attachments/assets/3d0f6581-262d-4dfd-99ea-a1c7e0baa8be" />

4. Install Python Dependencies

from requirements.txt

  pip install -r requirements.txt
  
Run Kafka Producer:

  python producer.py
  
Run Kafka Consumer:

  python consumer.py

Monitor Kafka

Kafka UI is available at http://localhost:8070
You can view the transactions topic in real time.

