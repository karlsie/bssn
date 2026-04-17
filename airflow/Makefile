up:
	docker-compose --env-file .env.local up -d airflow-init
	docker-compose --env-file .env.local --profile flower up -d

up-prod:
	docker-compose --env-file .env up -d airflow-init
	docker-compose --env-file .env --profile flower up -d

down:
	docker-compose down --volumes --rmi all

status:
	docker-compose ps
