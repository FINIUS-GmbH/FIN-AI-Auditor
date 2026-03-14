# Datenmodell

Das kanonische Ziel, die Produktgrenzen und der Delivery-Pfad sind im
[Zielbild](./target-picture.md) beschrieben. Diese Datei fokussiert auf die
lokale SSOT und die Modelle, die daraus technisch folgen.

## Grundidee

Der Auditor braucht ein eigenes kanonisches Datenmodell, das nicht mit dem FIN-AI Laufzeitmodell vermischt wird. Die lokale Entwicklungs-SSOT ist jetzt eine kleine SQLite-Datenbank. Sie dient nicht als Business-DB fuer FIN-AI, sondern als belastbarer Audit-Speicher fuer:

- ingestierte Quellensnapshots
- Findings mit Originalposition
- Relationen zwischen Findings
- spaeter Claims, Entscheidungen und Patch-Entwuerfe

Bis zu einer expliziten User-Entscheidung ist diese lokale Datenbank auch die einzige Komponente, die schreibend genutzt werden darf. Externe Systeme bleiben read-only.

## Aktueller lokaler Persistenzkern

### `audit_runs`

Beschreibt einen konkreten Audit-Lauf.

Wichtige Felder:

- `run_id`
- `status`
- `target_json`
- `created_at`
- `updated_at`
- `started_at`
- `finished_at`
- `summary`
- `error`

### `source_snapshots`

Speichert den beobachteten Stand einer Quelle zum Laufzeitpunkt.

Wichtige Felder:

- `snapshot_id`
- `run_id`
- `source_type`
- `source_id`
- `revision_id`
- `content_hash`
- `sync_token`
- `parent_snapshot_id`
- `collected_at`
- `metadata_json`

Zweck:

- Delta-Erkennung ueber `revision_id`, `content_hash` und `parent_snapshot_id`
- Reproduzierbarkeit eines Findings gegen den exakten Quellstand
- spaeter Grundlage fuer inkrementelle Re-Audits

### `audit_findings`

Speichert normalisierte Findings eines Audit-Laufs.

Wichtige Felder:

- `finding_id`
- `run_id`
- `severity`
- `category`
- `title`
- `summary`
- `recommendation`
- `canonical_key`
- `resolution_state`
- `proposed_confluence_action`
- `proposed_jira_action`
- `metadata_json`

`canonical_key` ist wichtig, um gleichartige Findings ueber mehrere Laeufe wiederzuerkennen.

### `finding_locations`

Speichert die originale Evidenzposition je Finding.

Wichtige Felder:

- `location_id`
- `finding_id`
- `snapshot_id`
- `source_type`
- `source_id`
- `title`
- `path_hint`
- `url`
- `anchor_kind`
- `anchor_value`
- `section_path`
- `line_start`
- `line_end`
- `char_start`
- `char_end`
- `snippet_hash`
- `content_hash`
- `metadata_json`

Damit laesst sich ein Finding spaeter auf:

- die exakte Datei und Zeilenrange
- eine Confluence-Section
- einen Metamodell-Abschnitt
- oder einen lokalen Doku-Block

zurueckfuehren.

### `finding_links`

Speichert Beziehungen zwischen Findings.

Aktuell vorgesehene Relationstypen:

- `contradicts`
- `supports`
- `duplicates`
- `depends_on`
- `gap_hint`
- `resolved_by`

Zweck:

- Widerspruchscluster bilden
- Erklaerungsketten fuer Luecken aufbauen
- spaeter Aufloesungen und Entscheidungen propagieren

## Kanonische In-Memory-Modelle

Aktuell existieren bereits diese Pydantic-Modelle:

- `AuditRun`
- `AuditSourceSnapshot`
- `AuditFinding`
- `AuditLocation`
- `AuditPosition`
- `AuditFindingLink`

Diese Modelle sind die aktuelle SSOT zwischen API, Worker und lokaler Persistenz.

Bereits real umgesetzt sind inzwischen zusaetzlich:

- `AuditClaimEntry`
- `TruthLedgerEntry`
- `DecisionPackage`
- `DecisionProblemElement`
- `DecisionRecord`
- `WritebackApprovalRequest`
- `RetrievalSegment`
- `RetrievalSegmentClaimLink`
- `AtlassianOAuthStateRecord`
- `AtlassianOAuthTokenRecord`
- `AuditImplementedChange`

## Retrieval-Index

Fuer schnelle Kontextbildung und Delta-Neubewertung fuehrt der Auditor lokal zusaetzlich:

- `retrieval_segments`
- `retrieval_segment_claim_links`

Ein `RetrievalSegment` haelt:

- stabilen Anchor
- segmentierten Textausschnitt
- Keywords
- optionales Embedding
- Delta-Status gegen den letzten abgeschlossenen Lauf

Die Claim-Links machen spaetere Fragen moeglich wie:

- welche Segmente stuetzen einen Claim?
- welche Claims muessen nach einer geaenderten Seite neu bewertet werden?
- welche Textfenster sollen fuer LLM-Empfehlungen priorisiert werden?

## Zielerweiterungen fuer die naechste Ausbaustufe

### Claim

Kanonische atomare Aussage.

Beispiele:

- Objekt existiert
- Property existiert
- Lifecycle-Regel gilt
- Read-Pfad ist dokumentiert
- Write-Pfad ist implementiert
- Scope-Regel ist verbindlich

Empfohlene Felder spaeter:

- `claim_id`
- `snapshot_id`
- `claim_type`
- `subject_kind`
- `subject_key`
- `predicate`
- `normalized_value`
- `confidence`
- `source_anchor`
- `normalization_version`

### Decision

Antwort auf eine offene Rueckfrage oder Governance-Entscheidung.

Empfohlene Felder:

- `decision_id`
- `decision_type`
- `resolution`
- `rationale`
- `decided_by`
- `applies_to_claim_ids`
- `supersedes_decision_id`

### Truth

Kanonische, lokal gespeicherte User- oder System-Wahrheit, die kuenftige Re-Audits beeinflusst.

Empfohlene Felder:

- `truth_id`
- `canonical_key`
- `subject_kind`
- `subject_key`
- `predicate`
- `normalized_value`
- `scope_kind`
- `scope_key`
- `truth_status`
- `source_kind`
- `created_from_problem_id`
- `supersedes_truth_id`

### ConfluencePatchDraft

Vorbereitete Doku-Aenderung.

Empfohlene Felder:

- `patch_id`
- `page_id`
- `section_anchor`
- `before_fragment`
- `after_fragment`
- `review_markup_mode`
- `approval_state`

### JiraTicketDraft

Vorbereiteter Ticket-Entwurf.

Empfohlene Felder:

- `ticket_draft_id`
- `project_key`
- `summary`
- `description_adf_or_markdown`
- `acceptance_criteria`
- `implementation_prompt`
- `finding_ids`

## Persistenzempfehlung

Fuer den aktuellen lokalen Aufbau ist SQLite richtig:

- klein
- robust
- null Betriebsaufwand
- gut fuer Runs, Snapshots, Findings und Delta-Metadaten

Fuer spaetere Team-Nutzung ist PostgreSQL die wahrscheinlich bessere Ziel-DB. Die Modellierung bleibt dabei relational; ein Graph als Audit-SSOT ist fuer diesen Anwendungsfall nicht noetig.
