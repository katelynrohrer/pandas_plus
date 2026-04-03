IMAGE_NAME = pdp_container

.PHONY: build run run-full run-baseline run-baseline-full track-main track-main-full track-baseline track-baseline-full

build:
	docker build -t $(IMAGE_NAME) .


run:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/main.py'

run-full:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/main.py'


run-baseline:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/baseline.py'

run-baseline-full:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/baseline.py'


track-main:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/track_metrics.py main'

track-main-full:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/track_metrics.py main'


track-baseline:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/track_metrics.py baseline'

track-baseline-full:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/track_metrics.py baseline'
