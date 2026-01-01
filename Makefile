BINS := producer consumer api cache-refresher

.PHONY: build
build:
	@mkdir -p bin
	@for bin in $(BINS); do \
		go build -o bin/$$bin ./cmd/$$bin; \
	done

.PHONY: run
run:
	@for bin in $(BINS); do \
		go run ./cmd/$$bin & \
	done; \
	wait
