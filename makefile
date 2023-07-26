.SILENT:

all: test mypy

test:
	poetry run pytest

mypy:
	# sometimes mypy reports false positive due to stale cache
	rm -rf .mypy_cache
	poetry run mypy
