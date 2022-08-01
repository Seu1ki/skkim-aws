cd /home/ec2-user/deploy &&
chmod +x airflow.sh
mkdir -p ./dags ./logs ./plugins &&
echo -e "AIRFLOW_UID=$(id -u)" > .env &&
docker-compose up airflow-init &&
docker-compose up
