IMAGE_NAME = pdp_container

.PHONY: build run run-full run-baseline run-baseline-full track-pdp track-pdp-full track-baseline track-baseline-full

build:
	docker build -t $(IMAGE_NAME) .

get-data:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/get_data.py'


run:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/main.py'

run-full:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/main.py'


run-baseline:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/baseline.py'

run-baseline-sort:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/baseline/sort.py'

run-baseline-full:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/baseline.py'
