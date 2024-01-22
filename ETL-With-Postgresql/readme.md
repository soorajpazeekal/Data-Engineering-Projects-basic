

# ETL-With-Postgresql
#### A beginner friendly data engineering project that process "Citi Bike Trip Histories" (https://citibikenyc.com/system-data) dataset and write to a Postgresql database. 




## It containes:

- Reading from file system [https://s3.amazonaws.com/tripdata/index.html]
- Pre-processing and compressing with spark-python (Pyspark)
- Writting to postgresql database [aws rds]
- Workflow and pipeline management with airflow 
- Visualization with streamlit [Direct sql query] 
- Deployment with Docker


## Installation

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/soorajpazeekal/Data-Engineering-Projects-basic)

##### Make sure git, miniconda, python 3.10, pyspark, airflow, streamlit are installed! If yes, Please follow 

```bash
  git clone <repo link>
```
```bash
  cd to <repo>
```
#### Set-up Database locally or (use any cloud services)
```bash
  docker compose up -d
```

```bash
  conda create -n myenv python=3.10
  conda activate myenv
```
```bash
  pip install -r requirements.txt
```
```bash
  Edit 'config.ini file' with own database credentials.
  Rename 'config.ini.example' to 'config.ini'
```
### This project uses Airflow for its data orchestration workflows. You can run airflow on your local machine for testing and development! 
> Note: You can run Airflow in different ways (using pip or Docker). A recommended way to install Airflow is to use the [astro-cli](https://docs.astronomer.io/astro/cli/install-cli) tool; it also supports Docker deployment.

#### With Astro-CLI (Linux)
```bash
curl -sSL install.astronomer.io | sudo bash -s -- v1.20.1
```
#### After Astro is successfully installed
```bash
astro dev start
```
> Note: Before starting this airflow project please create your config.ini file from config.ini.example file. You can found this in "airflow/include" and this main root directory
#### To stop or remove astro airflow
```bash
astro dev stop
astro dev kill
```

## Dashboard Visualizations
> Note: Before starting dashboard please create your config.ini file from config.ini.example file.
```bash
cd dashboard
docker build -t dashboard_app .
```
```bash
docker run --name dashboard_con -p 8501:8501 dashboard_app
```
```bash
docker start dashboard_con
```

## High-level Design

![Design](https://github.com/soorajpazeekal/Data-Engineering-Projects-basic/blob/main/ETL-With-Postgresql/Documents/High-level-design.png?raw=true)


## Project Screenshot

![Screenshot](https://github.com/soorajpazeekal/Data-Engineering-Projects-basic/blob/main/ETL-With-Postgresql/Documents/screenshot.png?raw=true)


