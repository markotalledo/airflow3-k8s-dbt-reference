.PHONY: setup test lint cluster deploy pools images backfill clean

setup:
	uv sync

test:
	uv run pytest -q

lint:
	uv run ruff check .
	uv run ruff format --check .

cluster:
	kind create cluster --config k8s/kind-config.yaml

deploy:
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade --install airflow apache-airflow/airflow -n airflow --create-namespace -f k8s/values.yaml
	kubectl apply -f k8s/rbac.yaml
	$(MAKE) pools

pools:
	kubectl -n airflow cp pools.json $$(kubectl -n airflow get pod -l component=scheduler -o name | head -1 | cut -d/ -f2):/tmp/pools.json
	kubectl -n airflow exec deploy/airflow-scheduler -- airflow pools import /tmp/pools.json

images:
	docker build -t ghcr.io/markotalledo/clean-events:0.1.0 docker/clean-events
	docker build -t ghcr.io/markotalledo/dbt-shop:0.1.0 docker/dbt
	kind load docker-image ghcr.io/markotalledo/clean-events:0.1.0 ghcr.io/markotalledo/dbt-shop:0.1.0 --name airflow

# Usage: make backfill FROM=2026-01-01 TO=2026-01-31
backfill:
	kubectl -n airflow exec deploy/airflow-scheduler -- airflow backfill create \
		--dag-id shop_daily --from-date $(FROM) --to-date $(TO) --max-active-runs 1 --reprocess-behavior completed

clean:
	kind delete cluster --name airflow
