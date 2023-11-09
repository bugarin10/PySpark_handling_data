# Makefile for Python Project requested a basic, readable makefile from chatGPT to understand how these funciton

# Variables
PYTHON := python3
VENV := env
SRC_DIR := project/src
TEST_DIR := project/tests
REQUIREMENTS := requirements.txt

# Default target

.PHONY: all env install

# Install project dependencies

env:
	$(PYTHON) -m venv $(VENV)

install:
	$(VENV)/bin/pip install --upgrade pip -r  $(REQUIREMENTS)

# Format code with Black
format:
	$(VENV)/bin/black $(SRC_DIR)

# Lint code with Flake8
lint:
	$(VENV)/bin/pylint $(SRC_DIR)

# Clean up generated files and virtual environment
clean:
	rm -rf $(VENV) __pycache__ .pytest_cache

all:
	env install clean