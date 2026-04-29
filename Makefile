IMAGE_NAME = pdp_container

# setup
build:
	docker build -t $(IMAGE_NAME) .

get-data:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/get_data.py'

# pdp
run:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/main.py'

run-full:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'python3 -u src/main.py'

# unsorted baseline
run-baseline:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/unsorted_baseline/baseline.py'

# sorted baseline
run-baseline-sort:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/sorted_baseline/sort.py'

run-baseline-insert:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/sorted_baseline/insert.py'

run-baseline-delete:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/sorted_baseline/delete.py'

run-baseline-lookup:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/sorted_baseline/lookup.py'

run-baseline-filter:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/sorted_baseline/filter.py'

run-baseline-project:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/sorted_baseline/project.py'

run-baseline-count:
	docker run --rm -it -v "$$(pwd)":/work -w /work $(IMAGE_NAME) bash -lc 'ulimit -v 1048576 && python3 -u src/sorted_baseline/count.py'
