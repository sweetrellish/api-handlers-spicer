import os
import re


# Core modules now in src/core/
SRC_CORE_MODULES = [
    "app",
    "companycam_service",
    "marketsharp_service",
    "mapping_registry",
    "pending_queue",
    "security",
    "webhook_handler"
]

# Config modules in config/
CONFIG_MODULES = [
    "config",
    "gunicorn.conf"
]



# Dynamically determine project root and walk all subdirectories
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKIP_DIRS = {'.venv', '__pycache__', '.git', 'backups'}

def update_imports_in_file(filepath):
    with open(filepath, "r") as f:
        content = f.read()
    original = content
    # Update src.core modules
    for module in SRC_CORE_MODULES:
        content = re.sub(
            rf"from\s+{module}\s+import",
            f"from src.core.{module} import",
            content
        )
        content = re.sub(
            rf"import\s+{module}(\s|$)",
            f"import src.core.{module}\1",
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
            f"import config.{module}\1",
            content
        )
    # Update file path references for .json, .jsonl, .db files to use data/
    content = re.sub(r"(['\"])([a-zA-Z0-9_\-]+\.(json|jsonl|db))(['\"])", r"\1data/\2\4", content)
    if content != original:
        with open(filepath, "w") as f:
            f.write(content)
        print(f"Updated: {filepath}")

def main():
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # Remove unwanted directories from traversal
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and d != "CCAPI"]
        for file in files:
            if file.endswith(".py") and file != "fix_imports.py":
                update_imports_in_file(os.path.join(root, file))

if __name__ == "__main__":
    main()
