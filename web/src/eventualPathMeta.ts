type EventualMeta = {
  label: string;
  icon: string;
  tone: string;
};

const EVENTUAL_PATH_META: Record<string, EventualMeta> = {
  connector_ingestion_enqueue: { label: "Connector/Ingestion", icon: "🛰", tone: "connector" },
  manual_answer_enqueue: { label: "Manual Answer", icon: "🧾", tone: "manual" },
  phase_execution_enqueue: { label: "Phase Execution", icon: "⚙️", tone: "phase" },
  reaggregation_enqueue: { label: "Reaggregation", icon: "🔁", tone: "reaggregation" },
  upload_enqueue: { label: "Upload", icon: "📁", tone: "upload" },
  storage_reconcile_enqueue: { label: "Storage Reconcile", icon: "🗂", tone: "storage" },
  workflow_registry_enqueue: { label: "Registry", icon: "📚", tone: "registry" },
  ingestion_recovery_enqueue: { label: "Recovery", icon: "🩹", tone: "recovery" },
  run_start_enqueue: { label: "Run Start", icon: "🚦", tone: "run" },
  worker_failure_enqueue: { label: "Worker Failure", icon: "🛠", tone: "worker" },
  ui_action_dispatch_enqueue: { label: "UI Action", icon: "🖱", tone: "ui" },
  graph_system_enqueue: { label: "Graph System", icon: "🕸", tone: "graph" },
  generic_async_gap: { label: "Async", icon: "⏳", tone: "generic" },
};

export function eventualPathMeta(value: unknown): EventualMeta | null {
  const key = String(value ?? "").trim();
  return EVENTUAL_PATH_META[key] ?? null;
}
