#!/bin/bash
#This script takes in a file via command line argument and creates a backup of that file in
#a backup directory with a timestamp in the filename. The backup directory is created if it does not exist.

# Check if a file was provided as an argument
if [ -z "$1" ]; then
  echo "Usage: $0 <file_to_backup>"
  exit 1
fi

FILE_TO_BACKUP="$1"
BACKUP_DIR="backups"
TIMESTAMP=$(date +"%Y%m%d%H%M%S")
BACKUP_FILE="$BACKUP_DIR/$(basename "$FILE_TO_BACKUP")_$TIMESTAMP"

# Create the backup directory if it does not exist
mkdir -p "$BACKUP_DIR"
# Copy the file to the backup directory with the timestamp in the filename
cp "$FILE_TO_BACKUP" "$BACKUP_FILE"
echo "Backup of $FILE_TO_BACKUP created at $BACKUP_FILE"    


