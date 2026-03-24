# pandas_plus

Docker is necessary to safely and consistently limit the memory size.

Build docker container:
    `docker build -t pdp_container .`

Run:
    Limited to 1gb memory:
    `docker run --rm -it -v "$PWD":/work -w /work pdp_container bash -lc 'ulimit -v 1048576; python3 -u main.py'`
    No limit (8gb memory):
    `docker run --rm -it -v "$PWD":/work -w /work pdp_container bash -lc 'python3 -u main.py'`

(Note: I used ulimit -v rather than --memory because
Docker kills the process immediately when --memory limit is exceeded,
whereas I wanted the Pandas native error)

