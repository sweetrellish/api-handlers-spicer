#!/usr/bin/env python3
"""
This script will check the for the existence of database "pending_comment.db" backups 
"""
import os
import glob

# Adjust this path if your backups are stored elsewhere
backup_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
candidates = glob.glob(os.path.join(backup_dir, '**', '*pending*.db*'), recursive=True)
print("Possible backup files found:")
for path in candidates:
    print(path)
