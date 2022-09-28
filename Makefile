# =========================================
#       		VARIABLES
# -----------------------------------------

LINT_DIRS := gdw_engine/ tests/ _config.py
TO_CLEAN  := build pip-wheel-metadata/
TODAY := $(shell date '+%Y-%m-%d')
PYTEST_OPTS := ''
OS = $(shell uname -s)
VERSION=$(shell python setup.py --version | sed 's/\([0-9]*\.[0-9]*\.[0-9]*\).*$$/\1/')

PROJECT_ID ?= servier-ingestion-dev
REGION ?= europe-west1
PROJECT_NUMBER ?= $$(gcloud projects list --filter=${PROJECT_ID} --format="value(PROJECT_NUMBER)")
ARTIFACTS_BUCKET ?= servier-ingestion-artifactory-bucket-${PROJECT_NUMBER}
STAGING_BUCKET ?= servier-ingestion-staging-bucket-${PROJECT_NUMBER}
RAWDATA_BUCKET ?= servier-ingestion-rawdata-bucket-${PROJECT_NUMBER}

# =========================================
#       			HELP
# -----------------------------------------

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

# =========================================
#       		TARGETS
# -----------------------------------------

help: help
clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts
install-all: install install-dev ## install all dependencies
lint: lint ## run flake8 on gdw_engine/ for linting
format: format ## run black on gdw_engine/ for code formatting
typing: typing ## run mypy on gdw_engine/ for code typing check
test: test ## run code unittests
coverage: coverage ## run code coverage
package: package ## build a job wheel
freeze: freeze ## compile req.txt files
docs-build: docs-build ## build docs
docs-launch: docs-launch ## launch docs locally
security-baseline: security-baseline ## Check code vulnerabilities
complexity-baseline: complexity-baseline ## Check code complexity

# =========================================
#       	   COMPILE PIP
# -----------------------------------------

.PHONY: freeze
freeze: ## compile pip packages
	@pip3 install --upgrade pip
	@pip3 install pip-tools --upgrade
	@python3 -m piptools compile requirements/requirements.in --output-file=requirements/requirements.txt
	@python3 -m piptools compile requirements/requirements-dev.in --output-file=requirements/requirements-dev.txt

# =========================================
#       	INSTALL DEPENDENCIES
# -----------------------------------------

.PHONY: install
install: ## install main job dependencies
	@pip3 install --upgrade pip
	PIP_CONFIG_FILE=pip.conf pip install -r requirements/requirements.txt

.PHONY: install-dev
install-dev: ## install dev dependencies
	@pip3 install --upgrade pip
	PIP_CONFIG_FILE=pip.conf pip install -r requirements/requirements-dev.txt

# =========================================
#       		RUN TESTS
# -----------------------------------------

.PHONY: lint
lint: ## run flake8 on gdw_engine/ for linting
	@flake8 $(LINT_DIRS) --exclude tests/conftest.py

.PHONY: format
format:  ## run black on gdw_engine/ for code formatting
	@black --target-version py37 $(LINT_DIRS) -l 120

.PHONY: typing
typing:  ## run mypy on gdw_engine/ for code typing check
	mypy --ignore-missing-imports -p gdw_engine

.PHONY: test
test:  ## run tests
	pytest -vv tests/

.PHONY: coverage
coverage:  ## run code coverage
	coverage run --source=gdw_engine/ --branch -m pytest tests --junitxml=coverage/test.xml -v
	coverage report --omit=gdw_engine/cli.py,gdw_engine/quality/_config.py --fail-under 30


# =========================================
#       	CHECK VULNERABILITY
# -----------------------------------------

.PHONY: security-baseline
security-baseline: ## Check code vulnerabilities
	poetry run bandit -r --exit-zero -b bandit.baseline.json gdw_engine

.PHONY: complexity-baseline
complexity-baseline: ## Check code complexity
	$(info Maintenability index)
	poetry run radon mi lambdas
	$(info Cyclomatic complexity index)
	poetry run xenon --max-absolute C --max-modules C --max-average C lambdas

# =========================================
#       		BUILD PACKAGE
# -----------------------------------------

.PHONY: package
package: ## build a job wheel
	PYTHON_USER_FLAG=$(shell python -c "import sys; print('' if hasattr(sys, 'real_prefix') or hasattr(sys, 'base_prefix') else '--user')") && \
	python -m pip install $(PYTHON_USER_FLAG) --upgrade setuptools wheel && \
	python setup.py bdist_wheel
	cp dist/gdw_engine-$(VERSION)-py3-none-any.whl dist/gdw_engine-latest-py3-none-any.whl

	# build the wheel and tarball
	rm -rf *.egg-info build
	mkdir ./build/
	mkdir ./build/gdw_engine/
	mkdir ./build/requirements/
	find . -name '__pycache__' -exec rm -fr {} +
	cp -r dist/gdw_engine-$(VERSION)-py3-none-any.whl ./build/ && \
	cp -r dist/gdw_engine-latest-py3-none-any.whl ./build/ && \
	cp -r gdw_engine/* ./build/gdw_engine && \
	cp -r requirements/* ./build/requirements && \
	rm -rf ./build/lib && \
	rm -rf ./build/bdist.macosx-*
	cd ./build/ && \
	zip -r ../dist/gdw_engine-$(VERSION)-packages.zip . && \
	tar -zcvf ../dist/gdw_engine-$(VERSION)-packages.tar.gz . && \
	rm -rf build/*

# =========================================
#       		DEPLOY DOCS
# -----------------------------------------

.PHONY: deploy
deploy: ## build a job wheel
	gsutil cp -r /workspace/dist gs://${ARTIFACTS_BUCKET}/gdw/pyfiles/
	gsutil cp -r /workspace/gdw_engine gs://${ARTIFACTS_BUCKET}/gdw/pyfiles/

# =========================================
#       		BUILD DOCS
# -----------------------------------------

.PHONY: docs-build
docs-build: ## Build docs
	@cd scripts/ && ./build-docs.sh

# =========================================
#       		LAUNCH DOCS
# -----------------------------------------

.PHONY: docs-launch
docs-launch: ## Launch docs
	sphinx-autobuild docs docs/build/html

# =========================================
#       			CLEAR
# -----------------------------------------

.PHONY: clean-build
clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr .eggs/
	rm -fr dist/*.py
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -fr {} +

.PHONY: clean-pyc
clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -type d -name venv -prune -o -type d -name __pycache__ -print0 | xargs -0 rm -rf

.PHONY: clean-test
clean-test: ## remove test and coverage artifacts
	rm -f .coverage
	rm -fr coverage/
	rm -fr htmlcov/
	rm -fr .pytest_cache
	rm -fr .mypy_cache/