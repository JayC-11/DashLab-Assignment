#!/bin/bash

# Run the server in the background
python3 serverside.py &
sleep 3
# Run multiple clients in parallel
python3 clientside.py 1 &
python3 clientside.py 2 &
python3 clientside.py 3 &
# Add more clients as needed

wait

