.PHONY: install test run-data run-model run-producer run-streaming

VENV = .venv/bin
PYTHON = $(VENV)/python3
PIP = $(VENV)/pip

install:
	$(PIP) install -r requirements.txt

test:
	$(PYTHON) -m pytest tests/ -v

run-data:
	$(PYTHON) src/data_engineering.py

run-model:
	$(PYTHON) src/model_training.py

run-producer:
	$(PYTHON) src/producer.py

run-streaming:
	$(PYTHON) src/streaming_job.py

run-all: run-data run-model
	@echo "Data engineered and model trained! To run streaming, open two terminals and run 'make run-streaming' and 'make run-producer'."
