.PHONY: run-producer run-consumer_dsm

run-producer:
	. .venv/bin/activate && DD_DATA_STREAMS_ENABLED=true DD_TRACE_AGENT_URL=http://localhost:8126 DD_SERVICE=kinesis-dsm-producer ddtrace-run python kinesis/producer.py

run-consumer_dsm:
	. .venv/bin/activate && DD_DATA_STREAMS_ENABLED=true DD_TRACE_AGENT_URL=http://localhost:8126 DD_SERVICE=kinesis-dsm-consumer ddtrace-run python kinesis/consumer.py
