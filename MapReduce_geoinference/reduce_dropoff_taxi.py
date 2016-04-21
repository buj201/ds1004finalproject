#!/usr/bin/python

import sys

current_key = None
current_count = 0

for line in sys.stdin:

    line = line.strip()
    key, value = line.split('\t',1)

    if current_key == key:
        #Same key as previous line from sys.stdin
        current_count +=1

    if (current_key != key and current_key):
        #Different key than previous, but there was a previous line read
        #Should print and reset
        print current_key + '\t' + current_count
        current_count = 1
        current_key = key

    if not current_key:
        #First line read in from sys.stdin, so current_key = None
        current_key = key
        current_count = 1

#Flush last line:

print current_key + '\t' + current_count
