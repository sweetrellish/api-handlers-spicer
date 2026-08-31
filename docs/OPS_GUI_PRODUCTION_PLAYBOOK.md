# OPS GUI Production Playbook

## Purpose
Use this playbook to make visual and mechanical changes to the Ops GUI, validate locally, and ship safely to production via SCP.

## Source Of Truth
- Server repo: `/home/rellis/spicer`
- GUI app: `/home/rellis/spicer/API Handler Interactive GUI`
- Backend API module: `/home/rellis/spicer/src/ops_api.py`
- Main scripts folder: `/home/rellis/spicer/scripts`

## What To Edit
### Visual UI
- `API Handler Interactive GUI/src/app/App.tsx`
- `API Handler Interactive GUI/src/app/components/Header.tsx`
- `API Handler Interactive GUI/src/app/components/MainDashboard.tsx`
- `API Handler Interactive GUI/src/app/components/CategoryView.tsx`
- `API Handler Interactive GUI/src/app/components/ui/dialog.tsx`
- `API Handler Interactive GUI/public/*`

### Mechanical Behavior
- `API Handler Interactive GUI/src/app/data/menuData.ts`
- `API Handler Interactive GUI/src/app/App.tsx`
- `src/ops_api.py`
- `scripts/*.py`

## Local Development Workflow
1. Edit the files you need.
2. Validate Python backend changes:

```bash
cd "/Users/ryanellis/Dev Repo/spicer"
python3 -m py_compile src/ops_api.py scripts/recover_missed_comments.py
```

3. Validate frontend changes:

```bash
cd "/Users/ryanellis/Dev Repo/spicer/API Handler Interactive GUI"
npm run build:ops-gui --silent
```

## Action Template: List-Returning Action
### 1) Add menu item
File: `API Handler Interactive GUI/src/app/data/menuData.ts`

```ts
{ id: '2.9', label: 'My New List Action' }
```

### 2) Optional frontend params + dispatch
File: `API Handler Interactive GUI/src/app/App.tsx`

```ts
if (key === '2.9') {
  await executeAction(categoryId, actionId, { mode: 'list', limit: 100 });
  return;
}
```

### 3) Backend handler + mapping
File: `src/ops_api.py`

```py
if key == "2.9":
    return _action_my_new_list(params)


def _action_my_new_list(params: dict[str, Any]):
    rows = [{"id": 1, "name": "example"}]
    return "My new list loaded.", {"items": rows}
```

## Action Template: Summary-Returning Action
File: `src/ops_api.py`

```py
if key == "4.9":
    return _action_my_summary(params)


def _action_my_summary(params: dict[str, Any]):
    return "Summary computed.", {
        "ok": True,
        "records": 42,
        "warnings": 1,
        "status": "ready"
    }
```

## Action Template: Script-Running Action
### Script must be non-interactive
File: `scripts/my_script.py`

```py
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--non-interactive", action="store_true")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()
```

### Backend call pattern
File: `src/ops_api.py`

```py
if key == "5.9":
    run = _run_python_script([str(SCRIPTS_DIR / "my_script.py"), "--non-interactive"], timeout=600)
    ui = _script_ui_result(run)
    message = "Script completed." if ui["ok"] else "Script failed."
    return message, ui
```

## API Response Shape Contract
Always return one of these from handlers:

- List results:
```json
{ "items": [ ... ] }
```

- Row results:
```json
{ "rows": [ ... ] }
```

- Summary/script results:
```json
{
  "ok": true,
  "exitCode": 0,
  "stdoutSnippet": "...",
  "stderrSnippet": "..."
}
```

## Frontend Popup Rendering Rules
File: `API Handler Interactive GUI/src/app/App.tsx`
- `getActionRows` should check in order: `items`, `rows`, `files`, then first array-valued field fallback.
- `executeAction` should treat payload-level `ok: false` as failure.
- Dialog should show:
  - array sections,
  - scalar/object summary cards,
  - stdoutSnippet/stderrSnippet blocks.

## Production Deploy (SCP)
Run from local machine.

### Frontend only
```bash
cd "/Users/ryanellis/Dev Repo/spicer/API Handler Interactive GUI"
scp src/app/App.tsx rellis@10.8.0.1:"/home/rellis/spicer/API Handler Interactive GUI/src/app/App.tsx"
ssh rellis@10.8.0.1 "cd '/home/rellis/spicer/API Handler Interactive GUI' && npm run build:ops-gui --silent"
```

### Backend only
```bash
cd "/Users/ryanellis/Dev Repo/spicer"
scp src/ops_api.py rellis@10.8.0.1:"/home/rellis/spicer/src/ops_api.py"
scp scripts/recover_missed_comments.py rellis@10.8.0.1:"/home/rellis/spicer/scripts/recover_missed_comments.py"
```

Then restart backend service on server (privileged step):

```bash
sudo systemctl restart spicer-flask-api.service
systemctl is-active spicer-flask-api.service
```

## Production Verification
```bash
# Service
curl -s -o /dev/null -w "health %{http_code}\n" http://127.0.0.1:5001/health

# GUI shell
curl -s -o /dev/null -w "ops-gui %{http_code}\n" http://127.0.0.1:5001/ops-gui/

# Example action
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"categoryId":"2","actionId":"7","params":{"mode":"scan"}}' \
  http://127.0.0.1:5001/ops/execute
```

## Troubleshooting
- Popup blank:
  - API returned arrays under non-standard keys; extend fallback extraction.
- Success message but no effect:
  - Ensure backend payload includes `ok`, and frontend honors `ok: false`.
- Script EOF/input errors:
  - Script still prompts for input; add `--non-interactive` code path.
- GCLID script not found:
  - Use resolver fallback in backend to check multiple script locations.

## Rollback
### Frontend rollback
Rebuild server GUI from prior known-good commit or backup and verify `ops-gui` endpoint.

### Backend rollback
Restore prior `src/ops_api.py` and/or script file, restart service, re-test one action.

## Recommended Working Pattern
1. Implement one action end-to-end.
2. Validate with direct `/ops/execute` POST.
3. Verify popup in browser.
4. Move to next action.
