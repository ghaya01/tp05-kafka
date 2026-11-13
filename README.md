# TP05: Real-Time Data Streaming and Processing Using Apache Kafka

## 1. Project Description

This project demonstrates real-time data streaming and processing using Apache Kafka.

A Kafka Producer generates synthetic campus sensor data, including temperature, humidity, sensor ID, building name, and timestamp.

A Kafka Consumer receives and processes these events in real time, calculates the running average temperature, filters data from Building A, and generates alerts when the temperature reaches or exceeds 35°C.
The consumer also saves the filtered sensor events from Building A into a CSV file named `sensor_data.csv`.
## 2. Technologies Used

- Apache Kafka 3.9.1
- Docker and Docker Compose
- Python
- kafka-python
- JSON

## 3. Project Structure

```text
tp05-kafka/
├── compose.yaml
├── requirements.txt
├── producer.py
├── consumer.py
├── sensor_data.csv
├── README.md
└── screenshots/

```

## 4. Installation and Execution

### 4.1 Start Kafka

```bash
docker compose up -d
```

### 4.2 Activate Python Virtual Environment

```bash
source .venv/bin/activate
```

### 4.3 Install Dependencies

```bash
pip install -r requirements.txt
```

### 4.4 Run the Producer

```bash
python producer.py
```

The producer sends 100 sensor events to the `campus-sensors` topic.

### 4.5 Run the Consumer

Open another terminal in the project directory, activate the virtual environment, then execute:

```bash
python consumer.py
```

The consumer filters events from Building A, calculates the running average temperature, and displays an alert when the temperature is at least 35°C.

## 5. Kafka Configuration

- **Broker:** localhost:9092
- **Main topic:** campus-sensors
- **Number of partitions:** 3
- **Consumer group:** tp05-campus-monitor
- **Message format:** JSON

## 6. Additional Experiments

- Sending 100 sensor events.
- Calculating the running average temperature.
- Comparing different consumer groups.
- Testing topics with different numbers of partitions.
- Adding campus building information.
- Filtering events from Building A.

## 7. Stop Kafka

```bash
docker compose down
```

## 8. Screenshots

Screenshots demonstrating the Kafka setup, producer, consumer, topics, partitions, and consumer groups are available in the `screenshots/` directory.

