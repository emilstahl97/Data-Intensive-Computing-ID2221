create-network:
	docker network create wikiflow-network

run-producer:
	mvn clean package -pl producer
	docker-compose up --build

run-consumer:
	mvn clean package -pl consumer
	docker-compose -f docker-compose-analytics-consumer.yaml build
	docker-compose -f docker-compose-analytics-consumer.yaml up