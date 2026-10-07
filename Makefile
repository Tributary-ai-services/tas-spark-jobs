IMAGE_NAME := registry-api.tas.scharber.com/tas-spark-jobs

# One source of truth for the version (semver, see CLAUDE.md "Versioning &
# Release Policy"). The image tag IS the version; there is no :latest and no
# bare-sha tag, because a floating tag means "we cannot tell you what is
# running".
VERSION  := $(shell cat VERSION)
REVISION := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
CREATED  := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

.PHONY: build push deploy test lint

build:
	docker build \
		--label org.opencontainers.image.version=$(VERSION) \
		--label org.opencontainers.image.revision=$(REVISION) \
		--label org.opencontainers.image.created=$(CREATED) \
		-t $(IMAGE_NAME):$(VERSION) .

push: build
	docker push $(IMAGE_NAME):$(VERSION)
	@echo "Pin the manifest to the digest, not just the tag:"
	@docker inspect --format '{{index .RepoDigests 0}}' $(IMAGE_NAME):$(VERSION)

deploy:
	kubectl apply -f ../aether-shared/k8s-shared-infrastructure/spark/events-aggregator-sparkapp.yaml

test:
	python -m pytest tests/ -v

lint:
	python -m flake8 jobs/ --max-line-length=120
