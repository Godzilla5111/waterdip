## Run pytest for the module tests
pytest:
	set -e
	set -x
	pytest -p no:warnings ./tests/*

pytest_with_coverage:
	set -e
	set -x
	WD_IS_TESTING = true
	pytest -p no:warnings --cov ./tests/*

# Start Waterdip server
start_server:
	python -m waterdip

# Start Celery worker and beat togather
start_worker_beat:
	export OMP_NUM_THREADS=1 && \
	celery --app=waterdip.celery_app worker --beat --loglevel=info