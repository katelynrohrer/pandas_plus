# pandas_plus

Makefile builds the project. Docker is necessary to safely and consistently limit the memory size.

'make build' -- builds the Docker container \
'make run' -- runs the program with default memory constraint of 1gb \
'make run-full' -- runs the program without memory constraint (8gb)



## Run commands manually from the makefile:

Build docker container: \
    - `docker build -t pdp_container .`

Run: \
    - Limited to 1gb memory: \
        - `docker run --rm -it -v "$PWD":/work -w /work pdp_container bash -lc 'ulimit -v 1048576; python3 -u main.py'` \
    - No limit (8gb memory): \
        - `docker run --rm -it -v "$PWD":/work -w /work pdp_container bash -lc 'python3 -u main.py'` 

(Note: I used ulimit -v rather than "--memory" flag because \
Docker kills the process immediately when "--memory" limit is exceeded, \
whereas I wanted the Pandas native error)

