create-network:
	docker network create wikiflow-network

run-appliance:
	mvn clean package -pl producer
	docker-compose up --build

run-analytics-consumer:
	mvn clean package -pl consumer
	docker-compose -f docker-compose-analytics-consumer.yaml build
	docker-compose -f docker-compose-analytics-consumer.yaml up