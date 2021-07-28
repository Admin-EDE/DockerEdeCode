# Run parseCSVtoEDE.py in the container environment with provided arguments.
# For example,
# ./run.sh parse --help
# 
# Works unix-like systems (linux, mac)
# 
# TODO: add support for Windows

docker run -it --rm --name etl -v ${PWD}:/usr/src/ede -w /usr/src/ede edemineduc/etl python3 parseCSVtoEDE.py $@
