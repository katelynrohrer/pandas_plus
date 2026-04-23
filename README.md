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


Decisions I made:
Everything is strings (simplifies memory calculations)
Pandas usage is assumed to be larger than it may actually be (attempt to estimate to prevent error)
There is no cache to read files from, the only options are disk or memory
Docker uses the main environment for disk (to prevent significantly large docker containers)

There's a lot of overhead in pandas structure, I overestimated on how much space a file would need to avoid crashes
E.g. the 200MB file is just on the cusp of sometimes too big, 800MB is definitely too big

Reads current memory state to determine chunk size, but doesn't double check it on cache read (could be a problem later)
Insert and delete modify in place, filter returns the new obj