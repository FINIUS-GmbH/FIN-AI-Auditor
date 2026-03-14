# Roadmap

Die Roadmap ist aus dem [Zielbild](./target-picture.md) abgeleitet und beschreibt
die sinnvolle Lieferreihenfolge, nicht nur eine lose Wunschliste.

## Stand 0

Scaffold und Richtungsfestlegung.

Enthaelt:

- Repo-Struktur
- API-Basis
- Worker-Skelett
- Frontend-Skelett
- Doku-Grundlage

## Stand 1

### Ziel

Read-only Audit MVP.

### Lieferumfang

- GitHub Snapshot Collector
- Confluence Read Collector
- FIN-AI Metamodell Read Collector
- erste Claim-Extraktion fuer:
  - Objekte
  - Status/Lifecycle
  - Read-/Write-Pfade
  - Scope-Regeln
- einfache Finding-Regeln
- keine externen Schreibzugriffe; nur lokale Auditor-DB wird fortgeschrieben

## Stand 2

### Ziel

Review- und Entscheidungsworkflow.

### Lieferumfang

- Finding-Triage
- Rueckfragen
- Decision-Objekte
- Truth-Ledger fuer User-Spezifizierungen
- kleine Entscheidungspakete statt flacher Finding-Listen
- Claim-Neuberechnung nach Entscheidungen

## Stand 3

### Ziel

Confluence Patch Preview.

### Lieferumfang

- Abschnittsanker
- Before/After-Patches
- Review-Markierungen:
  - rot durchgestrichen
  - gelb markiert
  - gruen bestaetigt
- Preview vor Apply
- Apply bleibt gesperrt, bis der User explizit freigibt

Aktueller Ist-Stand:

- section-anchored Patch-Preview ist als lokales Approval-Artefakt implementiert
- Review-Snippets werden farblich fuer entfernen, korrigieren und bestaetigen vorbereitet
- externer Apply-Pfad ist technisch vorhanden, braucht aber noch echten OAuth-Consent und Live-Verifikation gegen FINAI-Seiten

## Stand 4

### Ziel

Jira Ticket Drafting.

### Lieferumfang

- Finding-Cluster zu Ticket-Drafts
- Acceptance Criteria
- konkreter Code-Aenderungsprompt
- Referenzierte Evidenzen
- Ticket-Erzeugung bleibt bis zur Freigabe lokal als Draft

Aktueller Ist-Stand:

- strukturierte AI-Coding-Briefs und Jira-ADF-Payloads sind im Approval-Flow integriert
- externer Jira-POST ist technisch vorhanden, braucht aber noch echten OAuth-Consent mit gewaehrtem `write:jira-work`

## Stand 5

### Ziel

Inkrementeller Produktivbetrieb.

### Lieferumfang

- Delta-Audits pro Commit oder Doku-Aenderung
- wiederverwendbare Policies
- Freigabe-Workflow
- optionale Automatisierung unter Governance-Grenzen

## Stand 6

### Ziel

Hybrid Retrieval und portierte LiteLLM-Schicht.

### Lieferumfang

- Segment- und Claim-Index
- SQLite FTS5 als lexikale Suchbasis
- lokale Embedding-Caches fuer Segmente und Claims
- Hybrid Retrieval aus Struktur, Lexik und Semantik
- portierte LiteLLM-Kernbausteine aus FIN-AI ohne Runtime-Kopplung

Aktueller Ist-Stand:

- portierte LiteLLM-Kernbausteine sind bereits im Auditor integriert
- ein lokaler Retrieval-Index fuer Segmente, Claim-Verknuepfungen, SQLite-FTS5 und optionale Embeddings ist umgesetzt
- hybrides Kontext-Ranking mischt bereits Struktur-, Lexik- und Semantiksignale; tiefere Re-Ranking-Strategien und groessere Kontextheuristiken bleiben als naechste Ausbaustufe offen
- lokale Dokument-Caches, ADF-/Storage-Normalisierung und erste inkrementelle Wiederverwendung unveraenderter Repo-/Confluence-Quellen sind umgesetzt
- source-lokale Claim-/Finding-Wiederverwendung fuer unveraenderte Folgequellen ist als erste inkrementelle Reanalyse-Stufe umgesetzt; section-level Claim-Regeneration fuer geaenderte Confluence-Seiten ist ebenfalls da, transitive Minimal-Neubewertung bleibt aber noch nicht final minimal
- Worker-Claiming, owner-aware Lease-Heartbeats, stale-run-Recovery-Sicht sowie strukturierte Runtime-Logs und lokale Runtime-Metriken sind fuer den lokalen Mehrworker-Betrieb gehaertet, ohne schon den finalen Recovery-Stand zu erreichen
- Confluence-Analysis-Cache mit Seiten-Registry, Retention-Regeln, Restriktions-/Sensitivitaetsmetadaten und Abschnitts-Deltas ist lokal umgesetzt; Restriktions- und Sensitivitaetssignale werden dabei konservativ aus expliziten Restriktionen, Properties und Labels hergeleitet

## Aktueller Schwerpunkt

Die aktuell priorisierten Luecken zum Zielbild sind:

- realer Atlassian-OAuth-Consent mit verifizierten `granted_scopes` fuer Live-Read und spaetere kontrollierte Writebacks
- produktive End-to-End-Verifikation des vorhandenen Jira- und Confluence-Writebacks unter realem OAuth-Consent
- breitere strukturierte Claim-Extraktion aus verbleibenden komplexen Confluence-Storage/ADF-Sonderfaellen, weiteren Config-/Frontend-Mustern und Guardrail-Vertraegen
- breiterer direkter Metamodell-Read und tiefere modellgetriebene Semantik fuer weitere Fachobjekte und transitive Konflikte
- voll inkrementeller Betrieb ueber alle transitiv betroffenen Cluster, tieferes Fehlertracing sowie staerkere Recovery-/Lease-Strategien fuer mehrere Worker
