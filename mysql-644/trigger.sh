#!/bin/sh

# call runtran to trigger the bug

./runtran --repeat --seed 65323445 --database test --trace trace.txt --monitor pinot --thread 9 --host localhost 30 360 1 results

