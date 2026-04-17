IMAGE_NAME := registry-api.tas.scharber.com/tas-spark-jobs
SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo "latest")

.PHONY: build push deploy test lint

build:
	docker build -t $(IMAGE_NAME):$(SHA) -t $(IMAGE_NAME):latest .

push: build
	docker push $(IMAGE_NAME):$(SHA)
	docker push $(IMAGE_NAME):latest

deploy:
	kubectl apply -f ../aether-shared/k8s-shared-infrastructure/spark/events-aggregator-sparkapp.yaml

test:
	python -m pytest tests/ -v

lint:
	python -m flake8 jobs/ --max-line-length=120
