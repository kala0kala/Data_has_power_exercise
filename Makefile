start:
	docker-compose up -d
stop:
	docker-compose down
run-job:
	docker-compose exec spark-master bash -c '/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark-apps/mysql-connector-java-8.0.28.jar --driver-memory 1G --executor-memory 1G /opt/spark-apps/main.py'
init-db:
	docker-compose exec demo-database bash -c 'mysql -u root -pmy-secret-pw < table_creation.sql'
db-bash:
	docker-compose exec demo-database bash