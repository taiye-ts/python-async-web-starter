PROJECT_PY_FILES=`cd src && find -E . -type f -regex "^.*.py"`

.PHONY: build
build:
	@#@ build docker image
	@docker-compose build
	@echo Done


.PHONY: up
up:
	@#@ Run docker-compose up
	@docker-compose up -d --build
	@echo Done


.PHONY: logs
logs:
	@#@ Run logs from docker
	@docker-compose logs -f


.PHONY: lint
lint:
	@#@ Run linter inside VM or prepared environment from the project root.
	@docker-compose exec backend pylint --rcfile=/python/.pylintrc $(PROJECT_PY_FILES)
	@echo Done


.PHONY: bash
bash:
	@#@ Run bash in container
	@docker-compose exec backend bash


.PHONY: typecheck
typecheck:
	@#@ Run typechecking using mypy inside VM or prepared environment from the project root.
	@docker-compose exec backend mypy --ignore-missing-imports --config-file=/python/mypy.ini $(PROJECT_PY_FILES)
	@echo Done


.PHONY: db_branches
db_branches:
	@#@ Get list of migration branches
	@docker-compose exec backend alembic --name=main -c /app/project_name/storage/database/migrations/alembic.ini branches
	@echo Done


.PHONY: db_revision
db_revision:
	@#@ Create new migration, example: make db_revision initial
	@docker-compose exec backend alembic --name=main -c /app/project_name/storage/database/migrations/alembic.ini revision -m $(filter-out $@,$(MAKECMDGOALS))
	@echo Done


.PHONY: db_upgrade
db_upgrade:
	@#@ Upgrade to latest migration
	@docker-compose exec backend alembic --name=main -c /app/project_name/storage/database/migrations/alembic.ini upgrade head
	@echo Done


.PHONY: db_downgrade
db_downgrade:
	@#@ Downgrade to previous migration
	@docker-compose exec backend alembic --name=main -c /app/project_name/storage/database/migrations/alembic.ini downgrade -1
	@echo Done


.PHONY: shell
shell:
	@#@ Opens ipython shell
	@docker-compose exec backend ipython


.PHONY: dbshell
dbshell:
	@#@ Opens ipython database shell
	@docker-compose exec backend PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U docker -w


.PHONY: tests
tests:
	@#@ Opens ipython database shell
	@docker-compose exec backend python -m unittest discover


