.PHONY: install test run-producer run-streaming

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v

run-producer:
	python3 src/producer.py

run-streaming:
	python3 src/streaming_job.py
