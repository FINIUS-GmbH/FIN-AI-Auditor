import type {
  AtlassianAuthStatus,
  AtlassianAuthorizationStart,
  AuditRun,
  AuditTarget,
  BootstrapData,
  ConfluenceVerificationResponse,
} from "./types";

const API_BASE = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "";

async function parseResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}

export async function listAuditRuns(): Promise<AuditRun[]> {
  const response = await fetch(`${API_BASE}/api/audits/runs`);
  const body = await parseResponse<{ items: AuditRun[] }>(response);
  return body.items;
}

export async function createAuditRun(target: AuditTarget): Promise<AuditRun> {
  const response = await fetch(`${API_BASE}/api/audits/runs`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ target }),
  });
  return parseResponse<AuditRun>(response);
}

export async function getBootstrapData(): Promise<BootstrapData> {
  const response = await fetch(`${API_BASE}/api/bootstrap`);
  return parseResponse<BootstrapData>(response);
}

export async function getAtlassianAuthStatus(): Promise<AtlassianAuthStatus> {
  const response = await fetch(`${API_BASE}/api/ingestion/atlassian/auth/status`);
  return parseResponse<AtlassianAuthStatus>(response);
}

export async function startAtlassianAuthorization(): Promise<AtlassianAuthorizationStart> {
  const response = await fetch(`${API_BASE}/api/ingestion/atlassian/auth/start`);
  return parseResponse<AtlassianAuthorizationStart>(response);
}

export async function verifyConfluenceAccess(spaceKey: string): Promise<ConfluenceVerificationResponse> {
  const response = await fetch(
    `${API_BASE}/api/ingestion/atlassian/confluence/verify?space_key=${encodeURIComponent(spaceKey)}`,
  );
  return parseResponse<ConfluenceVerificationResponse>(response);
}

export interface ConfluencePageNode {
  id: string;
  title: string;
  parentId: string;
}

export interface ConfluencePageTree {
  space_key: string;
  space_name: string;
  pages: ConfluencePageNode[];
}

export async function listConfluencePages(spaceKey: string): Promise<ConfluencePageTree> {
  const response = await fetch(
    `${API_BASE}/api/ingestion/atlassian/confluence/pages?space_key=${encodeURIComponent(spaceKey)}&max_pages=100`,
  );
  return parseResponse<ConfluencePageTree>(response);
}

export async function submitDecisionComment(runId: string, commentText: string): Promise<AuditRun> {
  const response = await fetch(`${API_BASE}/api/audits/runs/${runId}/decision-comments`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      comment_text: commentText,
      related_finding_ids: [],
    }),
  });
  return parseResponse<AuditRun>(response);
}

export async function submitPackageDecision(
  runId: string,
  packageId: string,
  action: "accept" | "reject" | "specify",
  commentText?: string,
): Promise<AuditRun> {
  const response = await fetch(`${API_BASE}/api/audits/runs/${runId}/packages/${packageId}/decisions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      action,
      comment_text: commentText ?? null,
    }),
  });
  return parseResponse<AuditRun>(response);
}

export async function createWritebackApprovalRequest(
  runId: string,
  payload: {
    target_type: "confluence_page_update" | "jira_ticket_create";
    title: string;
    summary: string;
    target_url?: string | null;
    related_package_ids: string[];
    related_finding_ids: string[];
    payload_preview: string[];
  },
): Promise<AuditRun> {
  const response = await fetch(`${API_BASE}/api/audits/runs/${runId}/approval-requests`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return parseResponse<AuditRun>(response);
}

export async function resolveWritebackApprovalRequest(
  runId: string,
  approvalRequestId: string,
  decision: "approve" | "reject" | "cancel",
  commentText?: string,
): Promise<AuditRun> {
  const response = await fetch(
    `${API_BASE}/api/audits/runs/${runId}/approval-requests/${approvalRequestId}/decision`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        decision,
        comment_text: commentText ?? null,
      }),
    },
  );
  return parseResponse<AuditRun>(response);
}

export async function recordConfluencePageUpdate(
  runId: string,
  payload: {
    approval_request_id: string;
    page_title: string;
    page_url: string;
    changed_sections: string[];
    change_summary: string[];
    related_finding_ids: string[];
  },
): Promise<AuditRun> {
  const response = await fetch(`${API_BASE}/api/audits/runs/${runId}/implemented-changes/confluence-page-updated`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return parseResponse<AuditRun>(response);
}

export async function recordJiraTicketCreated(
  runId: string,
  payload: {
    approval_request_id: string;
    ticket_key: string;
    ticket_url: string;
    related_finding_ids: string[];
  },
): Promise<AuditRun> {
  const response = await fetch(`${API_BASE}/api/audits/runs/${runId}/implemented-changes/jira-ticket-created`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return parseResponse<AuditRun>(response);
}

export async function executeConfluencePageWriteback(
  runId: string,
  approvalRequestId: string,
): Promise<AuditRun> {
  const response = await fetch(
    `${API_BASE}/api/audits/runs/${runId}/approval-requests/${approvalRequestId}/execute/confluence-page`,
    {
      method: "POST",
    },
  );
  return parseResponse<AuditRun>(response);
}

export async function executeJiraTicketWriteback(
  runId: string,
  approvalRequestId: string,
): Promise<AuditRun> {
  const response = await fetch(
    `${API_BASE}/api/audits/runs/${runId}/approval-requests/${approvalRequestId}/execute/jira-ticket`,
    {
      method: "POST",
    },
  );
  return parseResponse<AuditRun>(response);
}
