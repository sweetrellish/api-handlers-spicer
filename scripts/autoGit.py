"""
Automate git add, commit (with message based on file changes), pull --rebase, and push.
Handles new/untracked files and errors gracefully.
"""
import subprocess
import sys
import os

def run(cmd, check=True, capture_output=True, text=True):
    try:
        result = subprocess.run(cmd, check=check, capture_output=capture_output, text=text)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed: {' '.join(cmd)}\n{e.stderr}")
        sys.exit(1)


def get_changed_files():
    # Get staged, unstaged, and untracked files
    status = run(["git", "status", "--porcelain"])
    files = []
    for line in status.splitlines():
        if not line.strip():
            continue
        path = line[3:]
        files.append(path)
    return files


def summarize_changes(files):
    # Try to generate a commit message based on file diffs
    if not files:
        return "No changes to commit."
    summary = []
    for f in files:
        if not os.path.exists(f):
            continue
        try:
            with open(f, "r", encoding="utf-8", errors="ignore") as file:
                lines = file.readlines()
                if lines:
                    summary.append(f"Update {f}: {lines[0].strip()[:60]}")
                else:
                    summary.append(f"Update {f}")
        except Exception:
            summary.append(f"Update {f}")
    return "\n".join(summary)


def main():
    try:
        print("[INFO] Adding all changes (including new files)...")
        run(["git", "add", "-A"])

        changed_files = get_changed_files()
        if not changed_files:
            print("[INFO] No changes to commit.")
            return

        print("[INFO] Generating commit message...")
        commit_msg = summarize_changes(changed_files)
        print(f"[INFO] Commit message:\n{commit_msg}\n")

        print("[INFO] Committing...")
        run(["git", "commit", "-m", commit_msg])

        print("[INFO] Pulling with rebase from origin...")
        run(["git", "pull", "--rebase"])

        print("[INFO] Pushing to origin...")
        run(["git", "push"])

        print("[SUCCESS] All changes committed and pushed.")
    except Exception as e:
        print(f"[FATAL] Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

