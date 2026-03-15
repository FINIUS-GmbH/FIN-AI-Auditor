import { useEffect, useRef, useState } from "react";
import type { AuditRun, ClarificationThread, ClarificationMessage } from "../types";
import {
  createClarificationThread,
  sendClarificationMessage,
  confirmTruthFromClarification,
  captureIndicationFromClarification,
  supersedeTruthFromClarification,
  dismissClarificationThread,
} from "../api";

/* ============================================================
   Props
   ============================================================ */

type ClarificationPanelProps = {
  run: AuditRun;
  packageId: string;
  /** Called with the updated run after any mutation */
  onRunUpdated: (run: AuditRun) => void;
};

/* ============================================================
   Helpers
   ============================================================ */

const PURPOSE_LABEL: Record<string, string> = {
  truth_clarification: "Wahrheitsklärung",
  rating_explanation: "Bewertungserklärung",
  action_routing: "Maßnahmen-Routing",
};

const STATUS_LABEL: Record<string, string> = {
  active: "Aktiv",
  resolved: "Abgeschlossen",
  dismissed: "Verworfen",
};

function relativeTime(isoDate: string): string {
  const diff = Date.now() - new Date(isoDate).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "gerade eben";
  if (mins < 60) return `vor ${mins} Min.`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `vor ${hrs} Std.`;
  return `vor ${Math.floor(hrs / 24)} Tag(en)`;
}

/* ============================================================
   Component
   ============================================================ */

export function ClarificationPanel({
  run,
  packageId,
  onRunUpdated,
}: ClarificationPanelProps): JSX.Element {
  const [expanded, setExpanded] = useState(false);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Find existing thread for this package
  const thread: ClarificationThread | null =
    run.clarification_threads.find(
      (t) => t.package_id === packageId && t.status === "active"
    ) ??
    run.clarification_threads.find((t) => t.package_id === packageId) ??
    null;

  const isResolved = thread?.status === "resolved" || thread?.status === "dismissed";

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    if (scrollRef.current && expanded) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [thread?.messages.length, expanded]);

  /* ---- Actions ---- */

  async function handleStartThread(): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      const updated = await createClarificationThread(run.run_id, {
        package_id: packageId,
        purpose: "truth_clarification",
      });
      onRunUpdated(updated);
      setExpanded(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Fehler beim Starten");
    } finally {
      setBusy(false);
    }
  }

  async function handleSendMessage(): Promise<void> {
    if (!thread || !input.trim()) return;
    setBusy(true);
    setError(null);
    try {
      const updated = await sendClarificationMessage(run.run_id, thread.thread_id, input.trim());
      onRunUpdated(updated);
      setInput("");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Fehler beim Senden");
    } finally {
      setBusy(false);
    }
  }

  async function handleConfirmTruth(msg: ClarificationMessage): Promise<void> {
    if (!thread) return;
    // Extract truth info from message metadata or content
    const meta = msg.metadata ?? {};
    const canonicalKey = (meta.proposed_canonical_key as string) || packageId;
    const normalizedValue = (meta.proposed_normalized_value as string) || msg.content;
    const subjectKind = (meta.proposed_subject_kind as string) || "package";
    const subjectKey = (meta.proposed_subject_key as string) || packageId;
    const predicate = (meta.proposed_predicate as string) || "clarification_truth";

    setBusy(true);
    setError(null);
    try {
      const updated = await confirmTruthFromClarification(run.run_id, thread.thread_id, {
        truth_canonical_key: canonicalKey,
        truth_normalized_value: normalizedValue,
        subject_kind: subjectKind,
        subject_key: subjectKey,
        predicate,
      });
      onRunUpdated(updated);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Fehler bei Bestätigung");
    } finally {
      setBusy(false);
    }
  }

  async function handleCaptureIndication(msg: ClarificationMessage): Promise<void> {
    if (!thread) return;
    setBusy(true);
    setError(null);
    try {
      const updated = await captureIndicationFromClarification(
        run.run_id,
        thread.thread_id,
        msg.content,
      );
      onRunUpdated(updated);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Fehler beim Speichern");
    } finally {
      setBusy(false);
    }
  }

  async function handleSupersede(msg: ClarificationMessage): Promise<void> {
    if (!thread) return;
    const meta = msg.metadata ?? {};
    const existingTruthId = (meta.conflicting_truth_id as string) || "";
    if (!existingTruthId) return;

    setBusy(true);
    setError(null);
    try {
      const updated = await supersedeTruthFromClarification(run.run_id, thread.thread_id, {
        existing_truth_id: existingTruthId,
        new_canonical_key: (meta.proposed_canonical_key as string) || packageId,
        new_normalized_value: (meta.proposed_normalized_value as string) || msg.content,
        new_subject_kind: (meta.proposed_subject_kind as string) || "package",
        new_subject_key: (meta.proposed_subject_key as string) || packageId,
        new_predicate: (meta.proposed_predicate as string) || "clarification_truth",
      });
      onRunUpdated(updated);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Fehler beim Ersetzen");
    } finally {
      setBusy(false);
    }
  }

  async function handleDismiss(): Promise<void> {
    if (!thread) return;
    setBusy(true);
    setError(null);
    try {
      const updated = await dismissClarificationThread(run.run_id, thread.thread_id);
      onRunUpdated(updated);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Fehler beim Verwerfen");
    } finally {
      setBusy(false);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>): void {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      void handleSendMessage();
    }
  }

  /* ---- Render helpers ---- */

  function renderMessage(msg: ClarificationMessage, idx: number): JSX.Element {
    const isSystem = msg.role === "system" || msg.role === "assistant";
    const isUser = msg.role === "user";

    return (
      <div
        key={msg.message_id || idx}
        className={`clar-msg ${isSystem ? "clar-msg-system" : ""} ${isUser ? "clar-msg-user" : ""}`}
      >
        <div className="clar-msg-head">
          <span className="clar-msg-role">
            {isSystem ? "System" : "Sie"}
          </span>
          <span className="clar-msg-time">{relativeTime(msg.created_at)}</span>
        </div>
        <div className="clar-msg-body">{msg.content}</div>

        {/* Reference badges */}
        {(msg.referenced_claim_ids.length > 0 ||
          msg.referenced_truth_ids.length > 0 ||
          msg.referenced_finding_ids.length > 0) && (
          <div className="clar-msg-refs">
            {msg.referenced_claim_ids.map((id) => (
              <span key={id} className="clar-ref-badge clar-ref-claim">Claim</span>
            ))}
            {msg.referenced_truth_ids.map((id) => (
              <span key={id} className="clar-ref-badge clar-ref-truth">Truth</span>
            ))}
            {msg.referenced_finding_ids.map((id) => (
              <span key={id} className="clar-ref-badge clar-ref-finding">Finding</span>
            ))}
          </div>
        )}

        {/* Outcome badge */}
        {msg.outcome_type && (
          <div className="clar-msg-outcome">
            <span className={`clar-outcome-badge clar-outcome-${msg.outcome_type}`}>
              {msg.outcome_type === "truth_confirmed" && "◆ Wahrheit bestätigt"}
              {msg.outcome_type === "truth_superseded" && "◆ Wahrheit ersetzt"}
              {msg.outcome_type === "indication_captured" && "◇ Indiz erfasst"}
              {msg.outcome_type === "context_only" && "○ Kontext gespeichert"}
              {msg.outcome_type === "conflict_kept" && "⚠ Widerspruch bleibt"}
            </span>
          </div>
        )}

        {/* Action buttons for truth confirmation / conflict resolution */}
        {msg.message_type === "truth_confirmation" && !msg.outcome_type && !isResolved && (
          <div className="clar-msg-actions">
            <button
              className="btn clar-btn-truth"
              disabled={busy}
              onClick={() => void handleConfirmTruth(msg)}
            >
              ◆ 100% sicher — Als Wahrheit
            </button>
            <button
              className="btn clar-btn-indication"
              disabled={busy}
              onClick={() => void handleCaptureIndication(msg)}
            >
              ◇ Nur als Hinweis
            </button>
          </div>
        )}

        {msg.message_type === "conflict_resolution" && !msg.outcome_type && !isResolved && (
          <div className="clar-msg-actions">
            <button
              className="btn clar-btn-supersede"
              disabled={busy}
              onClick={() => void handleSupersede(msg)}
            >
              Bestehende ersetzen
            </button>
            <button
              className="btn clar-btn-keep"
              disabled={busy}
              onClick={() => void handleCaptureIndication(msg)}
            >
              Beide behalten (Widerspruch)
            </button>
          </div>
        )}
      </div>
    );
  }

  /* ---- Main render ---- */

  // No thread yet — show start button
  if (!thread) {
    return (
      <div className="clar-panel clar-panel-empty">
        <button
          className="btn clar-btn-start"
          disabled={busy}
          onClick={() => void handleStartThread()}
        >
          {busy ? "Startet…" : "Klärung starten"}
        </button>
      </div>
    );
  }

  // Thread exists — collapsible panel
  return (
    <div className={`clar-panel ${expanded ? "clar-panel-open" : ""} ${isResolved ? "clar-panel-resolved" : ""}`}>
      {/* Toggle header */}
      <button
        className="clar-toggle"
        onClick={() => setExpanded(!expanded)}
      >
        <span className="clar-toggle-icon">{expanded ? "▾" : "▸"}</span>
        <span className="clar-toggle-label">
          Klärungsdialog
        </span>
        <span className={`clar-status-badge clar-status-${thread.status}`}>
          {STATUS_LABEL[thread.status] || thread.status}
        </span>
        <span className="clar-toggle-purpose">
          {PURPOSE_LABEL[thread.purpose] || thread.purpose}
        </span>
        <span className="clar-toggle-count">
          {thread.messages.length} Nachr.
        </span>
      </button>

      {/* Expanded content */}
      {expanded && (
        <div className="clar-body">
          {/* Messages */}
          <div className="clar-messages" ref={scrollRef}>
            {thread.messages.map((msg, i) => renderMessage(msg, i))}
          </div>

          {/* Resolution summary */}
          {thread.resolution_summary && (
            <div className="clar-resolution">
              <div className="clar-resolution-label">Ergebnis</div>
              <div className="clar-resolution-text">{thread.resolution_summary}</div>
              {thread.triggered_delta_recompute && (
                <span className="clar-delta-badge">Delta-Recompute ausgelöst</span>
              )}
            </div>
          )}

          {/* Error display */}
          {error && <div className="clar-error">{error}</div>}

          {/* Input area (only for active threads) */}
          {thread.status === "active" && (
            <div className="clar-input-area">
              <textarea
                className="clar-input"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Antwort eingeben… (Enter = Senden)"
                rows={2}
                disabled={busy}
              />
              <div className="clar-input-actions">
                <button
                  className="btn clar-btn-send"
                  disabled={busy || !input.trim()}
                  onClick={() => void handleSendMessage()}
                >
                  {busy ? "…" : "Senden"}
                </button>
                <button
                  className="btn clar-btn-dismiss"
                  disabled={busy}
                  onClick={() => void handleDismiss()}
                >
                  Ohne Ergebnis schließen
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
