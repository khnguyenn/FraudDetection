# Fraud Detection RabbitMQ Pipeline

A simple RabbitMQ pipeline that processes transaction data through your trained fraud detection model.

## Quick Start

### 1. Start RabbitMQ

```bash
docker-compose up -d
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Pipeline

**Terminal 1 - Start Consumer (processes transactions):**

```bash
cd src
python consumer.py
```

**Terminal 2 - Start Results Viewer (shows results):**

```bash
cd src
python results_viewer.py
```

**Terminal 3 - Send Sample Data:**

```bash
cd src
python producer.py
```

## What It Does

1. **Producer** (`producer.py`): Reads sample transactions from `sample_data.csv` and sends them to RabbitMQ
2. **Consumer** (`consumer.py`):
   - Gets transactions from queue
   - Runs them through your preprocessor and model in `/artifacts/`
   - Sends results to results queue
3. **Results Viewer** (`results_viewer.py`): Displays fraud detection results in real-time

## Sample Data

The pipeline includes 4 sample transactions in `sample_data.csv` with different risk profiles.

## RabbitMQ Management

Access the RabbitMQ management interface at: http://localhost:15672

- Username: admin
- Password: password

## Files Structure

```
├── src/
│   ├── producer.py      # Sends transaction data
│   ├── consumer.py      # Processes with your model
│   └── results_viewer.py # Shows results
├── artifacts/           # Your trained model & preprocessor
├── sample_data.csv      # 4 sample transactions
├── requirements.txt     # Python dependencies
└── docker-compose.yml   # RabbitMQ setup
```
