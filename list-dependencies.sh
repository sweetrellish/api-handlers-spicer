#!/bin/bash

set -euo pipefail

# Usage:
#   ./list-dependencies.sh [project_root] [output_file]
# Examples:
#   ./list-dependencies.sh
#   ./list-dependencies.sh /home/rellis/spicer
#   ./list-dependencies.sh /home/rellis/spicer /tmp/spicer_dependencies.txt

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="${1:-$script_dir}"
output_file="${2:-$project_root/spicer_dependencies.txt}"

if [[ ! -d "$project_root" ]]; then
    echo "Error: Project root does not exist: $project_root" >&2
    exit 1
fi

if [[ -e "$output_file" && ! -w "$output_file" ]]; then
    echo "Error: Output file is not writable: $output_file" >&2
    exit 1
fi

python3 - "$project_root" "$output_file" <<'PY'
import ast
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

root = Path(sys.argv[1]).resolve()
out_file = Path(sys.argv[2]).resolve()

IGNORE_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv", ".mypy_cache", ".pytest_cache"}
IGNORE_DIR_NAMES = {
    "backups",
    "backup",
    "snapshot",
    "snapshots",
}
SCRIPT_FILE_EXTS = {".py", ".sh", ".bash", ".zsh", ".ps1", ".bat"}
CONFIG_FILE_NAMES = {".env", ".env.local", ".env.production", ".env.development"}
CONFIG_FILE_EXTS = {".env", ".ini", ".cfg", ".conf", ".toml", ".yaml", ".yml", ".json"}


def iter_python_files(base: Path):
    for path in sorted(base.rglob("*.py")):
        rel_parts = set(path.relative_to(base).parts)
        lowered_parts = {p.lower() for p in rel_parts}
        if lowered_parts & IGNORE_DIRS:
            continue
        if lowered_parts & IGNORE_DIR_NAMES:
            continue
        if any("snapshot" in p or "backup" in p for p in lowered_parts):
            continue
        yield path


py_files = list(iter_python_files(root))
py_rel = [p.relative_to(root).as_posix() for p in py_files]

# Build module index for local python resolution.
module_to_file = {}
for rel in py_rel:
    if rel.endswith("/__init__.py"):
        pkg = rel[:-12].replace("/", ".")
        if pkg:
            module_to_file[pkg] = rel
        continue

    mod = rel[:-3].replace("/", ".")
    module_to_file[mod] = rel


def resolve_module_to_local_file(module_name: str):
    if not module_name:
        return None
    if module_name in module_to_file:
        return module_to_file[module_name]

    parts = module_name.split(".")
    while parts:
        candidate = ".".join(parts)
        if candidate in module_to_file:
            return module_to_file[candidate]
        parts.pop()
    return None


def module_for_file(rel_path: str):
    if rel_path.endswith("/__init__.py"):
        return rel_path[:-12].replace("/", ".")
    return rel_path[:-3].replace("/", ".")


def resolve_relative_import(current_rel: str, level: int, module: str | None):
    cur_mod = module_for_file(current_rel)
    cur_parts = [p for p in cur_mod.split(".") if p]

    # For a file module a.b.c, level=1 means current package a.b
    package_parts = cur_parts[:-1]
    if level > 1:
        if level - 1 > len(package_parts):
            return None
        package_parts = package_parts[: -(level - 1)] if (level - 1) else package_parts

    if module:
        package_parts.extend([p for p in module.split(".") if p])

    if not package_parts:
        return None

    return resolve_module_to_local_file(".".join(package_parts))


def maybe_resolve_path_ref(current_file: Path, raw: str):
    value = raw.strip()
    if not value or "\n" in value:
        return None

    if value.startswith(("http://", "https://", "s3://")):
        return None

    base_name = os.path.basename(value)
    if base_name in CONFIG_FILE_NAMES:
        is_candidate = True
    else:
        suffix = Path(value).suffix.lower()
        is_candidate = suffix in SCRIPT_FILE_EXTS or suffix in CONFIG_FILE_EXTS

    if not is_candidate:
        return None

    p = Path(value)
    candidates = []

    if p.is_absolute():
        if p.exists():
            candidates.append(p)
    else:
        candidates.append((current_file.parent / p).resolve())
        candidates.append((root / p).resolve())

    for cand in candidates:
        try:
            rel = cand.relative_to(root)
            if cand.exists() and cand.is_file():
                return rel.as_posix()
        except ValueError:
            continue
    return None


deps_by_file = defaultdict(list)
reverse_usage = defaultdict(set)

for py in py_files:
    rel = py.relative_to(root).as_posix()
    content = py.read_text(encoding="utf-8", errors="replace")

    try:
        tree = ast.parse(content)
    except SyntaxError as exc:
        deps_by_file[rel].append(("parse_error", f"SyntaxError: {exc.msg} (line {exc.lineno})"))
        continue

    local_py_deps = set()
    file_ref_deps = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local_dep = resolve_module_to_local_file(alias.name)
                if local_dep:
                    local_py_deps.add(local_dep)
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                local_dep = resolve_relative_import(rel, node.level, node.module)
                if local_dep:
                    local_py_deps.add(local_dep)
            else:
                if node.module:
                    local_dep = resolve_module_to_local_file(node.module)
                    if local_dep:
                        local_py_deps.add(local_dep)

        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            resolved = maybe_resolve_path_ref(py, node.value)
            if resolved:
                file_ref_deps.add(resolved)

    for dep in sorted(local_py_deps):
        if dep != rel:
            deps_by_file[rel].append(("local_python", dep))
            reverse_usage[dep].add(rel)

    for dep in sorted(file_ref_deps):
        if dep != rel:
            kind = "script_or_config"
            deps_by_file[rel].append((kind, dep))
            reverse_usage[dep].add(rel)


def append_file_block(lines_out, rel):
    lines_out.append(f"FILE: {rel}")
    deps = deps_by_file.get(rel, [])
    if not deps:
        lines_out.append("  DEPENDENCIES: (none found)")
    else:
        lines_out.append("  DEPENDENCIES:")
        for kind, val in deps:
            if kind == "local_python":
                lines_out.append(f"    - [py] {val}")
            elif kind == "script_or_config":
                lines_out.append(f"    - [file] {val}")
            elif kind == "parse_error":
                lines_out.append(f"    - [warning] {val}")

    users = sorted(reverse_usage.get(rel, set()))
    lines_out.append("  FOOTNOTE - USED IN:")
    if users:
        for u in users:
            lines_out.append(f"    - {u}")
    else:
        lines_out.append("    - (not referenced by other scanned Python files)")
    lines_out.append("")


referenced_files = [rel for rel in py_rel if reverse_usage.get(rel)]
unreferenced_files = [rel for rel in py_rel if not reverse_usage.get(rel)]

lines = []
lines.append(f"Dependency Report for: {root}")
lines.append(f"Total Python files scanned: {len(py_rel)}")
lines.append(f"Priority files (used by other Python files): {len(referenced_files)}")
lines.append(f"Unreferenced Python files: {len(unreferenced_files)}")
lines.append("")

lines.append("=== PRIORITY: FILES USED BY OTHER SOURCES ===")
lines.append("")
for rel in referenced_files:
    append_file_block(lines, rel)

lines.append("=== UNREFERENCED PYTHON FILES (GROUPED) ===")
lines.append("")
for rel in unreferenced_files:
    append_file_block(lines, rel)

out_file.parent.mkdir(parents=True, exist_ok=True)
out_file.write_text("\n".join(lines), encoding="utf-8")

print(f"Wrote dependency report to: {out_file}")
PY