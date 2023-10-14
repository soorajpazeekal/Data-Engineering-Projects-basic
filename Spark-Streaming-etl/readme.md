
# Spark Structured Streaming 

Sam's Club is an American chain of membership-only retail warehouse clubs owned and operated by Walmart Inc. This is a beginner-friendly proof of concept that involves Spark Structured Streaming to read and process data in real time.

Note: For testing and learning purposes, this project uses the Spark Structured Streaming Socket Channel, which is not good for real-life production-ready applications.

![Highlevel design](https://via.placeholder.com/468x300?text=App+Screenshot+Here)

## Documentation

[First, please refer this link](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html), This project has two different modules to generate, processes, and write real-time data to the Casandra database (all the data in this example is fake and generated using [faker Python](https://github.com/joke2k/faker).

- #### producer.py (It generates random data from a faker module and creates a JSON payload)
- #### socket-server.py (This starts a TCP socket server session)
- #### consumer.py (Pyspark client that reads from the socket server also pre-processes data)

### Sample JSON

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

To run locally

```bash
git clone <this repo>
cd <this repo>
```
    