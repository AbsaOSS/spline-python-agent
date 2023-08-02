.SILENT:

PKG_NAME=$(shell echo `poetry version | sed 's/-/_/g' | cut -d' ' -f1`)
PKG_VERSION=$(shell poetry version -s)
PKG_FILE=$(PKG_NAME)-$(PKG_VERSION)-py3-none-any.whl

.PHONY: all clean test mypy prepare build install

# Default target
all: test mypy

clean:
	echo "Cleaning up..."
	rm -rf dist

test:
	echo "Running tests..."
	poetry run pytest

mypy:
	echo "Running mypy..."
	# sometimes mypy reports false positive due to stale cache
	rm -rf .mypy_cache
	poetry run mypy

prepare:
	echo "Preparing for build..."
	poetry install

build: clean prepare test mypy
	echo "Building the package..."
	poetry build

install:
	echo "Installing the package..."
	pip install dist/$(PKG_FILE) --force-reinstall
