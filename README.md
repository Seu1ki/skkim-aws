## Project using AWS (amazon web service)

The project activities are divided into two parts:   
### PART1. Data processing application   
Data processing application is based on Apache Airflow, an open-source workflow management platform for data pipelines.   
> airflow needs <.env> with   
```
AIRFLOW_UID=1000
AWS_ACCESS_KEY=<AWS_ACCESS_KEY>
AWS_SECRET_KEY=<AWS_SECRET_KEY>
DB_USER=<DB_USER>
DB_PASSWORD=<DB_PASSWORD>
DB_HOST=<DB_HOST>
DB_PORT=3306
DB_DB_NAME=<RDS_DB_NAME>
```
###PART2. Web application   
