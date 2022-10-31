build-production:
	@echo Building and tagging BACKEND
	docker build -t gzileni/coperncius-backend ./backend
	@echo Building and tagging FRONTEND
	docker build -t gzileni/coperncius-frontend ./frontend
	@echo --- build-production finished ---

publish-production:
	@echo Publishing
	docker-compose -f ./docker-compose.yml --env-file .env down
	docker-compose -f ./docker-compose.yml --env-file .env rm
	docker-compose -f ./docker-compose.yml --env-file .env build
	docker-compose -f ./docker-compose.yml --env-file .env up -d --remove-orphans
	@echo --- publish finished ---
