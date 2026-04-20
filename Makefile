IMAGE_NAME = pdp_container

.PHONY: build run run-full run-baseline run-baseline-full track-pdp track-pdp-full track-baseline track-baseline-full

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


track-pdp:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/track_metrics.py pdp'

track-pdp-full:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/track_metrics.py pdp'

track-baseline:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/track_metrics.py baseline'

track-baseline-full:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/track_metrics.py baseline'

test-pdp:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && export PYTHONPATH=/work/src && pytest -v tests/test_pdp.py'

test-baseline:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && export PYTHONPATH=/work/src && pytest -v tests/test_baseline.py'