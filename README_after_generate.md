# project_name

# Run dev env
```shell script
make build
make up
make db_upgrade
```

# Commands

```shell script
make build - builds docker iomage
make up - starts dev enviroment
make bash - opens bash session in container
make logs - opens docker logs
make lint - runs pylint
make typecheck - runs mypy typecheck
make db_branches - runs list of branches from alembic
make db_revision <name of migrations> creates new migration
make db_upgrade - upgrades to latest migration
make db_downgrade - downgrades to previous migration
make shell - opens ipython shell
make dbshell - opens database shell
make tests - starts unit tests
```


# IDE remote interpreter

## PyCharm Professional
While adding new interpreter select 'Docker Compose' and use from current image

## VSCode
Visit https://code.visualstudio.com/docs/remote/containers for more info
