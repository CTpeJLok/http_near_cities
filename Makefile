clear:
	clear


restart: down up
restart-build: down up-build

up:
	docker-compose up -d

up-build:
	docker-compose up -d --build

run:
	docker-compose up

run-build:
	docker-compose up --build


down:
	docker-compose down

down-ro:
	docker-compose down --remove-orphans


logs:
	docker-compose logs -f
