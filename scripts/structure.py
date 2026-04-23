import os
import shutil

# Define where each type of file should go
SRC_MODULES = [
    "pending_queue.py",
    "mapping_registry.py",
    "companycam_service.py",
    "marketsharp_service.py",
    "webhook_handler.py",
    "security.py",
    "posted_comments_audit.py",
    "true_fail_checker.py",
    "queue_ui_poster.py",
    "extract_and_map_users.py",
    "requeue_unmatched.py",
    "requeue_posted.py",
    "edit_unmatched_queue_item.py",
    "delete_queue_items_by_name.py",
    "upsert_contact_mapping.py",
    "list_unresolved_projects.py",
    "review_true_fail.py",
    "app.py"
]

CONFIG_FILES = [
    "config.py"
]

DATA_FILES = [
    "companycam_to_marketsharp_user_map.json",
    "marketsharp_contact_mappings.json",
    "marketsharp_cookies.json",
    "unmatched_comments.jsonl",
    "unmatched_comments_corrected.jsonl"
]

# Add more as needed

# Source and destination directories
BASE_DIR = os.path.expanduser("~/spicer")
SRC_DIR = os.path.join(BASE_DIR, "src")
CONFIG_DIR = os.path.join(BASE_DIR, "config")
DATA_DIR = os.path.join(BASE_DIR, "data")

# Ensure destination directories exist
def ensure_dirs():
    for d in [SRC_DIR, CONFIG_DIR, DATA_DIR]:
        os.makedirs(d, exist_ok=True)

def move_files(file_list, dest_dir):
    for fname in file_list:
        src_path = os.path.join(BASE_DIR, fname)
        if os.path.exists(src_path):
            dest_path = os.path.join(dest_dir, fname)
            print(f"Moving {src_path} -> {dest_path}")
            shutil.move(src_path, dest_path)
        else:
            print(f"File not found, skipping: {src_path}")

def main():
    ensure_dirs()
    move_files(SRC_MODULES, SRC_DIR)
    move_files(CONFIG_FILES, CONFIG_DIR)
    move_files(DATA_FILES, DATA_DIR)
    print("File move complete. Please review and run your import-fixing script next.")

if __name__ == "__main__":
    main()

