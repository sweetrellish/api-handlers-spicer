#!/bin/bash
#requires ./create_backup.sh to be in the same dir
for file in *; do
  if [ -f "$file" ]; then
    ./create_backup.sh "$file"
  fi
done

