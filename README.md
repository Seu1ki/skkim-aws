## Project using AWS (amazon web service)

The project activities are divided into two parts [Data processing application + Web application].   

![architecture_aws](https://user-images.githubusercontent.com/63331703/184807652-708365cb-5f76-46fc-a9c2-d0ae6a6d2a5e.png)

### PART1. Data processing application   
Data processing application is based on Apache Airflow, an open-source workflow management platform for data pipelines. Information is extracted by analyzing data based on statistical data of public data.

* src 0: Traffic accidents by road type and climate condition <https://www.data.go.kr/>
* res 0: the event most likely to occur
* res 1: Increment rate of the largest increase compared to the previous year
* res 2: cumulative number of incidents by road type

The data pipeline operation of airflow is written in data-pipeline/README.md.
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

### PART2. Web application   
The web application server provides visualization of database query results.   
It is based on Flask, a micro-web framework written in Python that can be written in a single file.

