BINS := producer consumer api

.PHONY: build
build:
	@mkdir -p bin
	@for bin in $(BINS); do \
		go build -o bin/$$bin ./cmd/$$bin; \
	done

.PHONY: run
run: build
	@for bin in $(BINS); do \
		echo "Running $$bin..."; \
		./bin/$$bin & \
	done