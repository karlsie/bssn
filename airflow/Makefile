dev:
	docker-compose --env-file .env.dev up -d airflow-init
	docker-compose --env-file .env.dev --profile flower up -d

prod:
	docker-compose --env-file .env up -d airflow-init
	docker-compose --env-file .env --profile flower up -d

down:
	docker-compose down --volumes --rmi all

status:
	docker-compose ps
