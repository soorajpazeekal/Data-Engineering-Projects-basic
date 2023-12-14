[![open Linkedin profile](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/soorajpazeekal)

# Spark Structured Streaming 

Sam's Club is an American chain of membership-only retail warehouse clubs owned and operated by Walmart Inc. This is a beginner-friendly proof of concept that involves Spark Structured Streaming to read and process data in real time.

Note: For testing and learning purposes, this project uses the Spark Structured Streaming Socket Channel, which is not good for real-life production-ready applications.

![Highlevel design](https://github.com/soorajpazeekal/Data-Engineering-Projects-basic/blob/main/Spark-Streaming-etl/documents/Untitled-2023-09-02-1214.png?raw=true)

## Documentation
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/soorajpazeekal/Data-Engineering-Projects-basic)

[First, please refer this link](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html), This project has two different modules to generate, processes, and write real-time data to the Casandra database (all the data in this example is fake and generated using [faker Python](https://github.com/joke2k/faker).

- #### producer.py (It generates random data from a faker module and creates a JSON payload)
- #### socket-server.py (This starts a TCP socket server session)
- #### consumer.py (Pyspark client that reads from the socket server also pre-processes data)

### Sample JSON payload

```yaml
{
  "Order_id": "56440a9d-29b6-4e01-b9af-33accd0db8d8",
  "Name": "Andrew Thompson",
  "Email": "patelnicholas@example.com",
  "Total_Price": 416,
  "Discount_or_Promotion": false,
  "PaymentMethod": "cash",
  "latitude": 40.0602681,
  "longitude": -82.917628055,
  "created_at": 1697270281.4114344
}
```

## Installation
If docker installed (it will spinup cassandra and socket-server):
```bash
git clone <this repo>
cd <this repo>
docker compose up -d
```
Then run cql file inside data-generator folder, 
```sql
CREATE KEYSPACE IF NOT EXISTS live_feed WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
```
To run locally, make sure Python 3.10, Spark 3.3.2, Findspark 2.0.1, Faker 19.12.0, and Cassandra Driver 3.28.0 are installed.

```bash
git clone <this repo>
cd <this repo>
```
### First run socket-server:
```bash
cd data-generator
Python socket-server.py
```
### Then run data consumer:
```bash
cd <root repo>
Python consumer.py
```


## Screenshots

![consumer.py Screenshot](https://github.com/soorajpazeekal/Data-Engineering-Projects-basic/blob/main/Spark-Streaming-etl/documents/Screenshot%202023-10-14%20203700.png?raw=true)


![consumer.py Screenshot](https://github.com/soorajpazeekal/Data-Engineering-Projects-basic/blob/main/Spark-Streaming-etl/documents/Screenshot%202023-10-14%20202041.png?raw=true)
    