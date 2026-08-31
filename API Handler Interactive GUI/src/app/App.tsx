import { useEffect, useMemo, useState } from 'react';
import { Header } from './components/Header';
import { MainDashboard } from './components/MainDashboard';
import { CategoryView } from './components/CategoryView';
import { menuData } from './data/menuData';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from './components/ui/dialog';

type ViewState =
  | { type: 'main' }
  | { type: 'category', categoryId: string };

type QueueStats = {
  pending: number;
  posted: number;
  active: number;
  unmatched: number;
  trueFail: number;
  processing: number;
};

type ApiResponse = {
  success: boolean;
  message: string;
  data?: any;
};

type ActionDialog =
  | { kind: 'webhookTest'; customerName: string; commentText: string }
  | { kind: 'recoverMentions'; hours: string; apply: boolean }
  | { kind: 'sinceDate'; categoryId: string; actionId: string; since: string }
  | { kind: 'confirmRepushPosted' }
  | { kind: 'confirmConsolidate' }
  | { kind: 'queueRename'; itemId: number; newName: string }
  | { kind: 'queueDelete'; itemId: number }
  | null;

type ActionResponseDialog = {
  title: string;
  message: string;
  categoryId: string;
  actionId: string;
  rows: any[];
  rawData: any;
  success: boolean;
} | null;

export default function App() {
  const [viewState, setViewState] = useState<ViewState>({ type: 'main' });
  const [queueStats, setQueueStats] = useState<QueueStats>({
    pending: 0,
    posted: 0,
    active: 0,
    unmatched: 0,
    trueFail: 0,
    processing: 0,
  });
  const [isBusy, setIsBusy] = useState(false);
  const [statusMessage, setStatusMessage] = useState('Ready.');
  const [actionData, setActionData] = useState<any>(null);
  const [lastRunAt, setLastRunAt] = useState<string | null>(null);
  const [dialog, setDialog] = useState<ActionDialog>(null);
  const [responseDialog, setResponseDialog] = useState<ActionResponseDialog>(null);

  const selectedCategory = useMemo(
    () => (viewState.type === 'category' ? menuData.categories.find((category) => category.id === viewState.categoryId) : null),
    [viewState]
  );

  const getActionLabel = (categoryId: string, actionId: string) => {
    const category = menuData.categories.find((item) => item.id === categoryId);
    const action = category?.actions.find((item) => item.id === actionId);
    return action?.label || `Action ${categoryId}.${actionId}`;
  };

  const getActionRows = (data: any) => {
    if (!data) {
      return [];
    }
    if (Array.isArray(data.items)) {
      return data.items;
    }
    if (Array.isArray(data.rows)) {
      return data.rows;
    }
    if (Array.isArray(data.files)) {
      return data.files;
    }
    // Fallback: show the first array-valued field from API payloads like duplicate scans.
    if (typeof data === 'object') {
      const firstArrayEntry = Object.entries(data).find(([, value]) => Array.isArray(value));
      if (firstArrayEntry) {
        const [group, value] = firstArrayEntry;
        return (value as any[]).map((row) =>
          typeof row === 'object' && row !== null ? { ...row, _group: group } : { value: row, _group: group }
        );
      }
    }
    return [];
  };

  const refreshQueueStats = async () => {
    try {
      const response = await fetch('/ops/queue/stats');
      const body: ApiResponse = await response.json();
      if (body.success && body.data) {
        setQueueStats({
          pending: Number(body.data.pending || 0),
          posted: Number(body.data.posted || 0),
          active: Number(body.data.active || 0),
          unmatched: Number(body.data.unmatched || 0),
          trueFail: Number(body.data.trueFail || 0),
          processing: Number(body.data.processing || 0),
        });
      }
    } catch {
      setStatusMessage('Unable to reach backend at /ops. Ensure Flask is running.');
    }
  };

  useEffect(() => {
    refreshQueueStats();
    const timer = setInterval(refreshQueueStats, 10000);
    return () => clearInterval(timer);
  }, []);

  const executeAction = async (categoryId: string, actionId: string, params: Record<string, any> = {}) => {
    setIsBusy(true);
    setStatusMessage('Executing operation...');
    try {
      const response = await fetch('/ops/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ categoryId, actionId, params }),
      });

      const body: ApiResponse = await response.json();
      const payload = body.data as any;
      const isPayloadFailure = Boolean(payload && typeof payload === 'object' && payload.ok === false);
      const isSuccess = Boolean(body.success) && !isPayloadFailure;

      setStatusMessage(body.message || (isSuccess ? 'Operation completed.' : 'Operation failed.'));
      setActionData(payload || null);
      setLastRunAt(new Date().toLocaleTimeString());
      setResponseDialog({
        title: getActionLabel(categoryId, actionId),
        message: body.message || (isSuccess ? 'Operation completed.' : 'Operation failed.'),
        categoryId,
        actionId,
        rows: getActionRows(payload),
        rawData: payload || null,
        success: isSuccess,
      });
      await refreshQueueStats();
      return body;
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Request failed';
      setStatusMessage(`Operation failed: ${message}`);
      setActionData(null);
      setResponseDialog({
        title: getActionLabel(categoryId, actionId),
        message,
        categoryId,
        actionId,
        rows: [],
        rawData: null,
        success: false,
      });
      return { success: false, message } as ApiResponse;
    } finally {
      setIsBusy(false);
    }
  };

  const handleCategorySelect = (categoryId: string) => {
    if (categoryId === 'quit') {
      setStatusMessage('Quit selected. Close browser tab to exit GUI session.');
      return;
    }
    setActionData(null);
    setStatusMessage('Ready.');
    setViewState({ type: 'category', categoryId });
  };

  const handleBack = () => {
    setActionData(null);
    setStatusMessage('Ready.');
    setViewState({ type: 'main' });
  };

  const handleActionSelect = async (categoryId: string, actionId: string) => {
    const key = `${categoryId}.${actionId}`;

    if (key === '1.1') {
      setDialog({ kind: 'webhookTest', customerName: 'Test Customer', commentText: 'GUI test comment' });
      return;
    }

    if (key === '2.4') {
      await executeAction(categoryId, actionId, { mode: 'list' });
      return;
    }

    if (key === '2.5') {
      setDialog({ kind: 'confirmRepushPosted' });
      return;
    }

    if (key === '2.6') {
      await executeAction(categoryId, actionId, { mode: 'list' });
      return;
    }

    if (key === '2.7') {
      await executeAction(categoryId, actionId, { mode: 'scan' });
      return;
    }

    if (key === '2.8') {
      setDialog({ kind: 'recoverMentions', hours: '24', apply: false });
      return;
    }

    if (key === '3.3' || key === '3.4' || key === '3.6') {
      setDialog({
        kind: 'sinceDate',
        categoryId,
        actionId,
        since: new Date().toISOString().slice(0, 10),
      });
      return;
    }

    if (key === '4.4') {
      setDialog({ kind: 'confirmConsolidate' });
      return;
    }

    if (key === '4.5') {
      await executeAction(categoryId, actionId, { mode: 'recent' });
      return;
    }

    await executeAction(categoryId, actionId);
  };

  const handleQueueItemAction = async (mode: 'requeue' | 'rename' | 'delete', item: any) => {
    if (mode === 'rename') {
      setDialog({ kind: 'queueRename', itemId: item.id, newName: item.customer_name || '' });
      return;
    }

    if (mode === 'delete') {
      setDialog({ kind: 'queueDelete', itemId: item.id });
      return;
    }

    await executeAction('2', '2', { mode, id: item.id });
    await executeAction('2', '2', { mode: 'list' });
  };

  const closeDialog = () => setDialog(null);
  const closeResponseDialog = () => setResponseDialog(null);

  const submitDialog = async () => {
    if (!dialog) {
      return;
    }

    if (dialog.kind === 'webhookTest') {
      await executeAction('1', '1', {
        customerName: dialog.customerName,
        commentText: dialog.commentText,
      });
      closeDialog();
      return;
    }

    if (dialog.kind === 'recoverMentions') {
      await executeAction('2', '8', {
        hours: Number(dialog.hours || '24'),
        apply: dialog.apply,
      });
      closeDialog();
      return;
    }

    if (dialog.kind === 'sinceDate') {
      await executeAction(dialog.categoryId, dialog.actionId, { since: dialog.since });
      closeDialog();
      return;
    }

    if (dialog.kind === 'confirmRepushPosted') {
      await executeAction('2', '5', { mode: 'all' });
      closeDialog();
      return;
    }

    if (dialog.kind === 'confirmConsolidate') {
      await executeAction('4', '4', { confirm: true });
      closeDialog();
      return;
    }

    if (dialog.kind === 'queueRename') {
      await executeAction('2', '2', {
        mode: 'rename',
        id: dialog.itemId,
        newName: dialog.newName,
      });
      await executeAction('2', '2', { mode: 'list' });
      closeDialog();
      return;
    }

    if (dialog.kind === 'queueDelete') {
      await executeAction('2', '2', { mode: 'delete', id: dialog.itemId });
      await executeAction('2', '2', { mode: 'list' });
      closeDialog();
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-[#050505] via-[#0a0a0a] to-[#0f0f10] text-gray-100">
      <div className="min-h-screen flex flex-col">
        <Header queueStats={queueStats} onBack={viewState.type === 'category' ? handleBack : undefined} />

        <main className="flex-1 px-6 py-8 max-w-7xl mx-auto w-full">
          {viewState.type === 'main' ? (
            <MainDashboard
              categories={menuData.categories}
              queueStats={queueStats}
              onCategorySelect={handleCategorySelect}
            />
          ) : (
            <CategoryView
              category={selectedCategory!}
              queueStats={queueStats}
              onActionSelect={handleActionSelect}
              onBack={handleBack}
            />
          )}

          <section className="mt-8 rounded-2xl border border-cyan-500/20 bg-black/25 p-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <h3 className="text-sm font-semibold text-cyan-300">Interactive Action Responses Enabled</h3>
                <p className="text-xs text-gray-400">Each action now opens a response window so users do not need to scroll through terminal output.</p>
              </div>
              <div className="text-right">
                <p className={isBusy ? 'text-yellow-300 text-sm' : 'text-emerald-300 text-sm'}>{isBusy ? 'Running action...' : 'Idle'}</p>
                <p className="text-[11px] text-gray-500">{lastRunAt ? `Last run at ${lastRunAt}` : 'No actions run yet'}</p>
              </div>
            </div>
            <p className="mt-3 text-sm text-gray-300">{statusMessage}</p>
          </section>

          <Dialog open={Boolean(dialog)} onOpenChange={(open) => (!open ? closeDialog() : undefined)}>
            <DialogContent className="bg-[#0b0f10] border border-[#00ff00]/30 text-gray-100 sm:max-w-xl">
              <DialogHeader>
                <DialogTitle className="text-[#00d4ff]">
                  {dialog?.kind === 'webhookTest' && 'Send Test Webhook'}
                  {dialog?.kind === 'recoverMentions' && 'Run Mention Recovery'}
                  {dialog?.kind === 'sinceDate' && 'Choose Start Date'}
                  {dialog?.kind === 'confirmRepushPosted' && 'Requeue Posted Items'}
                  {dialog?.kind === 'confirmConsolidate' && 'Consolidate Queue Databases'}
                  {dialog?.kind === 'queueRename' && `Rename Queue Item #${dialog.itemId}`}
                  {dialog?.kind === 'queueDelete' && `Delete Queue Item #${dialog.itemId}`}
                </DialogTitle>
                <DialogDescription className="text-gray-400">
                  {dialog?.kind === 'webhookTest' && 'Provide values for the synthetic CompanyCam webhook payload.'}
                  {dialog?.kind === 'recoverMentions' && 'Dry-run is safer. Enable apply only when you intend to send emails.'}
                  {dialog?.kind === 'sinceDate' && 'Use YYYY-MM-DD to scope report and audit operations.'}
                  {dialog?.kind === 'confirmRepushPosted' && 'This will move posted rows back to pending for replay.'}
                  {dialog?.kind === 'confirmConsolidate' && 'This operation merges queue DBs after creating backups.'}
                  {dialog?.kind === 'queueRename' && 'Renaming also requeues the item to pending.'}
                  {dialog?.kind === 'queueDelete' && 'This operation permanently removes the queue row.'}
                </DialogDescription>
              </DialogHeader>

              <div className="space-y-4">
                {dialog?.kind === 'webhookTest' && (
                  <>
                    <label className="block text-sm text-gray-300">
                      Customer Name
                      <input
                        className="mt-1 w-full rounded-md border border-gray-700 bg-black/40 px-3 py-2 text-sm text-gray-100"
                        value={dialog.customerName}
                        onChange={(event) => setDialog({ ...dialog, customerName: event.target.value })}
                      />
                    </label>
                    <label className="block text-sm text-gray-300">
                      Comment Text
                      <textarea
                        className="mt-1 w-full rounded-md border border-gray-700 bg-black/40 px-3 py-2 text-sm text-gray-100 min-h-24"
                        value={dialog.commentText}
                        onChange={(event) => setDialog({ ...dialog, commentText: event.target.value })}
                      />
                    </label>
                  </>
                )}

                {dialog?.kind === 'recoverMentions' && (
                  <>
                    <label className="block text-sm text-gray-300">
                      Lookback Hours
                      <input
                        type="number"
                        min="1"
                        className="mt-1 w-full rounded-md border border-gray-700 bg-black/40 px-3 py-2 text-sm text-gray-100"
                        value={dialog.hours}
                        onChange={(event) => setDialog({ ...dialog, hours: event.target.value })}
                      />
                    </label>
                    <label className="flex items-center gap-2 text-sm text-gray-300">
                      <input
                        type="checkbox"
                        checked={dialog.apply}
                        onChange={(event) => setDialog({ ...dialog, apply: event.target.checked })}
                      />
                      Apply mode (send emails)
                    </label>
                  </>
                )}

                {dialog?.kind === 'sinceDate' && (
                  <label className="block text-sm text-gray-300">
                    Start Date
                    <input
                      type="date"
                      className="mt-1 w-full rounded-md border border-gray-700 bg-black/40 px-3 py-2 text-sm text-gray-100"
                      value={dialog.since}
                      onChange={(event) => setDialog({ ...dialog, since: event.target.value })}
                    />
                  </label>
                )}

                {dialog?.kind === 'queueRename' && (
                  <label className="block text-sm text-gray-300">
                    New Customer Name
                    <input
                      className="mt-1 w-full rounded-md border border-gray-700 bg-black/40 px-3 py-2 text-sm text-gray-100"
                      value={dialog.newName}
                      onChange={(event) => setDialog({ ...dialog, newName: event.target.value })}
                    />
                  </label>
                )}
              </div>

              <DialogFooter>
                <button
                  onClick={closeDialog}
                  className="px-4 py-2 rounded-md border border-gray-600 text-gray-300 hover:bg-gray-800"
                >
                  Cancel
                </button>
                <button
                  onClick={submitDialog}
                  disabled={isBusy}
                  className="px-4 py-2 rounded-md border border-[#00ff00]/50 text-[#00ff00] hover:bg-[#00ff00]/10 disabled:opacity-60"
                >
                  {isBusy ? 'Running...' : 'Run Action'}
                </button>
              </DialogFooter>
            </DialogContent>
          </Dialog>

          <Dialog open={Boolean(responseDialog)} onOpenChange={(open) => (!open ? closeResponseDialog() : undefined)}>
            <DialogContent className="bg-[#0b0f10] border border-cyan-400/30 text-gray-100 sm:max-w-3xl">
              <DialogHeader>
                <DialogTitle className={responseDialog?.success ? 'text-cyan-300' : 'text-red-300'}>
                  {responseDialog?.title || 'Action Response'}
                </DialogTitle>
                <DialogDescription className="text-gray-400">{responseDialog?.message}</DialogDescription>
              </DialogHeader>

              <div className="space-y-4">
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                  <div className="rounded-lg border border-cyan-500/20 bg-black/35 px-3 py-2">
                    <p className="text-[11px] uppercase tracking-wide text-gray-400">Action</p>
                    <p className="text-sm text-cyan-200">{responseDialog?.categoryId}.{responseDialog?.actionId}</p>
                  </div>
                  <div className="rounded-lg border border-cyan-500/20 bg-black/35 px-3 py-2">
                    <p className="text-[11px] uppercase tracking-wide text-gray-400">Result</p>
                    <p className={responseDialog?.success ? 'text-sm text-emerald-300' : 'text-sm text-red-300'}>
                      {responseDialog?.success ? 'Success' : 'Failed'}
                    </p>
                  </div>
                  <div className="rounded-lg border border-cyan-500/20 bg-black/35 px-3 py-2">
                    <p className="text-[11px] uppercase tracking-wide text-gray-400">Rows Returned</p>
                    <p className="text-sm text-cyan-200">{responseDialog?.rows?.length || 0}</p>
                  </div>
                </div>

                {responseDialog?.rows?.length ? (
                  <div className="max-h-[24rem] overflow-auto rounded-lg border border-cyan-500/20 bg-black/35 p-3 space-y-2">
                    {responseDialog.rows.slice(0, 50).map((item: any, index: number) => (
                      <div
                        key={item.id || item.event_id || item.path || index}
                        className="rounded-md border border-cyan-500/15 bg-black/40 p-3"
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <p className="text-sm text-cyan-200">
                              {item.id ? `#${item.id} • ` : ''}
                              {item.status || item.group || 'row'}
                              {item.customer_name ? ` • ${item.customer_name}` : ''}
                            </p>
                            <p className="text-xs text-gray-400 mt-1 break-words">
                              {(item.comment_text || item.path || item.event_id || JSON.stringify(item)).slice(0, 220)}
                            </p>
                          </div>

                          {responseDialog.categoryId === '2' && responseDialog.actionId === '2' && item.id && (
                            <div className="flex gap-2">
                              <button
                                onClick={() => handleQueueItemAction('requeue', item)}
                                className="px-2 py-1 text-xs rounded border border-cyan-400/40 text-cyan-300 hover:bg-cyan-500/10"
                              >
                                Requeue
                              </button>
                              <button
                                onClick={() => handleQueueItemAction('rename', item)}
                                className="px-2 py-1 text-xs rounded border border-yellow-400/40 text-yellow-300 hover:bg-yellow-500/10"
                              >
                                Rename
                              </button>
                              <button
                                onClick={() => handleQueueItemAction('delete', item)}
                                className="px-2 py-1 text-xs rounded border border-red-400/40 text-red-300 hover:bg-red-500/10"
                              >
                                Delete
                              </button>
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="space-y-3">
                    {responseDialog?.rawData && typeof responseDialog.rawData === 'object' && (
                      <div className="rounded-lg border border-cyan-500/20 bg-black/35 p-3">
                        <p className="text-xs uppercase tracking-wide text-gray-400 mb-2">Details</p>
                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-sm">
                          {Object.entries(responseDialog.rawData)
                            .filter(([key]) => !['stdout', 'stderr', 'command'].includes(key))
                            .map(([key, value]) => (
                              <div key={key} className="rounded border border-gray-800 bg-black/40 px-2 py-1">
                                <span className="text-gray-500 mr-2">{key}:</span>
                                {Array.isArray(value) ? (
                                  <span className="text-cyan-200">{value.length} item(s)</span>
                                ) : typeof value === 'object' && value !== null ? (
                                  <span className="text-cyan-200">object</span>
                                ) : (
                                  <span className="text-cyan-200">{String(value)}</span>
                                )}
                              </div>
                            ))}
                        </div>
                      </div>
                    )}

                    {responseDialog?.rawData && typeof responseDialog.rawData === 'object' &&
                      Object.entries(responseDialog.rawData)
                        .filter(([, value]) => Array.isArray(value) && (value as any[]).length > 0)
                        .map(([key, value]) => (
                          <div key={key} className="rounded-lg border border-cyan-500/20 bg-black/35 p-3">
                            <p className="text-xs uppercase tracking-wide text-gray-400 mb-2">{key}</p>
                            <div className="space-y-2 max-h-48 overflow-auto">
                              {(value as any[]).slice(0, 20).map((row, index) => (
                                <pre
                                  key={`${key}-${index}`}
                                  className="text-xs text-gray-300 overflow-x-auto whitespace-pre-wrap rounded border border-gray-800 bg-black/40 p-2"
                                >
                                  {typeof row === 'object' ? JSON.stringify(row, null, 2) : String(row)}
                                </pre>
                              ))}
                            </div>
                          </div>
                        ))}

                    {(responseDialog?.rawData as any)?.stdoutSnippet && (
                      <div className="rounded-lg border border-gray-800 bg-black/40 p-3">
                        <p className="text-xs uppercase tracking-wide text-gray-400 mb-2">Output</p>
                        <pre className="text-xs text-gray-300 overflow-x-auto whitespace-pre-wrap">{(responseDialog.rawData as any).stdoutSnippet}</pre>
                      </div>
                    )}

                    {(responseDialog?.rawData as any)?.stderrSnippet && (
                      <div className="rounded-lg border border-red-500/20 bg-black/40 p-3">
                        <p className="text-xs uppercase tracking-wide text-red-300 mb-2">Warnings</p>
                        <pre className="text-xs text-red-200 overflow-x-auto whitespace-pre-wrap">{(responseDialog.rawData as any).stderrSnippet}</pre>
                      </div>
                    )}

                    {!responseDialog?.rawData && (
                      <p className="text-sm text-gray-300">No additional data returned.</p>
                    )}
                  </div>
                )}
              </div>

              <DialogFooter>
                <button
                  onClick={closeResponseDialog}
                  className="px-4 py-2 rounded-md border border-cyan-400/40 text-cyan-300 hover:bg-cyan-500/10"
                >
                  Close
                </button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </main>

        <footer className="border-t border-[#00ff00]/10 bg-black/20 backdrop-blur-sm">
          <div className="max-w-7xl mx-auto px-6 py-4 flex justify-between items-center text-sm">
            <span className="text-[#00d4ff]/70">Spicer Bros. Admin Console</span>
            <span className="text-gray-500">written by Ryan Ellis</span>
          </div>
        </footer>
      </div>
    </div>
  );
}
