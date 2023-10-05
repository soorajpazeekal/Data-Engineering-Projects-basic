
# ETL-With-Postgresql
#### A beginner friendly data engineering project that process "Citi Bike Trip Histories" (https://citibikenyc.com/system-data) dataset and write to a Postgresql database. 




## It containes:

- Reading from file system [https://s3.amazonaws.com/tripdata/index.html]
- Pre-processing and compressing with spark-python (Pyspark)
- Writting to postgresql database [aws rds]
- Workflow and pipeline management with airflow standalone 
- Visualization with streamlit [Direct sql query] 
- Deployment with Docker [Not included now!]


## Installation

##### Make sure git, python 3.10, pyspark, airflow, streamlit are installed! If yes, Please follow 

```bash
  git clone <repo link>
```
```bash
  cd to <repo>
```
```bash
  Edit 'config.ini - file' with own database credentials.
  Rename to 'config.ini - file' to 'config.ini'
```
##### Start airflow server, if using docker add this repo dir to the airflow dags path
```bash
airflow standalone or docker cmd
```
```bash
cd dashboard
streamlit run app.py
```


## High-level Design

![Design](https://github.com/soorajpazeekal/Data-Engineering-Projects-basic/blob/main/ETL-With-Postgresql/Documents/High-level-design.png?raw=true)


## Project Screenshot

![Screenshot](https://github.com/soorajpazeekal/Data-Engineering-Projects-basic/blob/main/ETL-With-Postgresql/Documents/screenshot.png?raw=true)

