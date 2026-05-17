#!/bin/bash
#This script will open a given .csv file from cli args

if [ -f "$1" ]; then
        column -s, -t < "$1" | less -S
fi

