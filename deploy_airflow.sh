cd /home/ec2-user/deploy &&
mkdir -p ./dags ./logs ./plugins &&
cp /home/ec2-user/.env.tmp /home/ec2-user/deploy/
#docker-compose up airflow-init &&
#docker-compose up
docker-compose restart airflow-init
