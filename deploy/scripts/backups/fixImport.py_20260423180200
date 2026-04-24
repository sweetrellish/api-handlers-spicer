import os
import re

# Modules that move to src/
SRC_MODULES = [
    "pending_queue",
    "mapping_registry",
    "companycam_service",
    "marketsharp_service",
    "webhook_handler",
    "security",
    "posted_comments_audit",
    "true_fail_checker",
    "queue_ui_poster",
    "extract_and_map_users",
    "requeue_unmatched",
    "requeue_posted",
    "edit_unmatched_queue_item",
    "delete_queue_items_by_name",
    "upsert_contact_mapping",
    "list_unresolved_projects",
    "review_true_fail",
    "app"
]

# Modules that move to config/
CONFIG_MODULES = [
    "config"
]

# Directories to process (excluding CCAPI)
BASE_DIRS = [
    os.path.expanduser("~/spicer"),
    os.path.expanduser("~/spicer/scripts")
]

def update_imports_in_file(filepath):
    with open(filepath, "r") as f:
        content = f.read()
    original = content
    # Update src modules
    for module in SRC_MODULES:
        content = re.sub(
            rf"from\s+{module}\s+import",
            f"from src.{module} import",
            content
        )
        content = re.sub(
            rf"import\s+{module}(\s|$)",
            f"import src.{module}\\1",
            content
        )
    # Update config modules
    for module in CONFIG_MODULES:
        content = re.sub(
            rf"from\s+{module}\s+import",
            f"from config.{module} import",
            content
        )
        content = re.sub(
            rf"import\s+{module}(\s|$)",
            f"import config.{module}\\1",
            content
        )
    if content != original:
        with open(filepath, "w") as f:
            f.write(content)
        print(f"Updated: {filepath}")

def main():
    for base_dir in BASE_DIRS:
        for root, dirs, files in os.walk(base_dir):
            # Skip CCAPI directory
            if "CCAPI" in dirs:
                dirs.remove("CCAPI")
            for file in files:
                if file.endswith(".py") and file != "fix_imports.py":
                    update_imports_in_file(os.path.join(root, file))

if __name__ == "__main__":
    main()
