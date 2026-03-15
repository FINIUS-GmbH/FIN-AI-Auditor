# Abschlussplan bis 100 % fachliche Nutzbarkeit, 100 % Feature-Completeness und 100 % Production-Readiness

## Zusammenfassung
- Der Plan deckt jetzt das **gesamte Zielbild** ab, nicht nur den Write-Scope. Der Write-Scope bleibt Priorität 1, aber alle Domänen, die für Vollabdeckung nötig sind, sind enthalten.
- Die drei von dir genannten Punkte sind **bereits umgesetzt** und zählen als erreichte Basis:
  - lokaler Variablenfluss für `repo/driver/session`
  - Vererbung und Interface-/Protocol-Mapping über Dateigrenzen
  - qualifizierte Repository-/Driver-Symbole in Decision Packages und Jira-Briefs
- Der Abschluss wird in drei harte Zielmarken geschnitten:
  1. **100 % fachlich-analytische Nutzbarkeit im fokussierten Scope**
  2. **100 % Feature-Completeness gegenüber dem Zielbild**
  3. **100 % Production-Readiness**

## Status jetzt
- Fachlich-analytische Nutzbarkeit im fokussierten Scope: **ca. 82 %**
- Feature-Completeness gegenüber dem Zielbild: **ca. 68 %**
- Production-Readiness: **ca. 42 %**

### Bereits erreicht und fest einzufrieren
- Python-Write-Graph: lokaler Alias-Fluss, intermodulare Import-/Symbolauflösung, Constructor Injection, Protocol-/Vererbungsauflösung, qualifizierte Adapter-/Driver-Symbole.
- Causal Graph: Write-Decider, DB-Write-API, Sink-Typen, Schema-Ziel, SSOT-/Observed-Status, Surfacing bis in Packages und Jira.
- Truth/Delta-Basis: One-shot-Delta-Recompute, Truth-Ledger-Vererbung, graphbasierte Root-Cause-Attribution.
- Basis-Retrieval, semantische Graph-Schicht, Paketbildung, Approval-Flow, Jira-/Confluence-Writeback-Pfade als technische Grundlage.

## Arbeitspakete bis 100 %

### 1. Gold-Set und messbare Qualitätssteuerung
**Status:** 20 %  
**Ziel:** Qualitätsnachweis statt Bauchgefühl.

- Ein kuratiertes Gold-Set anlegen mit:
  - echten FIN-AI-Widersprüchen
  - Klarfällen/Nicht-Konflikten
  - False-Positive-Fallen
  - Truth-/Delta-Fällen
  - Write-/Schema-Drift
  - PUML-/Tabellen-/Negationsfällen
  - AS-IS-vs-Zielbild-Fällen
- Pro Fall fest definieren:
  - erwartete Kategorie
  - erwarteter Primär-Root-Cause
  - erwartete Quellenpriorität
  - erwartete Paketbildung
  - erwartete Jira-/Patch-Zulässigkeit
- Metriken verbindlich machen:
  - kritische Recall im Gold-Set: **100 %**
  - High-Severity-Precision: **>= 90 %**
  - Negative Fälle mit P1/P2-False-Positives: **0**
  - Kernprobleme müssen in den Top-Findings erscheinen
- CI-Gate: `feature complete` erst bei grünem Gold-Set.

### 2. Claim- und Semantik-Härtung gegen echten Benchmark
**Status:** 55 %  
**Ziel:** Fehlinterpretationen systematisch herausnehmen.

- BSM-/PUML-/Doku-Extraktion auf Assertionsmodell umstellen:
  - `asserted`, `excluded`, `deprecated`, `not_ssot`, `secondary_only`, `allowed_set`, `forbidden_set`, `status_set`
- Markdown-Tabellen, Listen, Legends, Notes, Negationen und PUML-Kontexte strukturiert parsen.
- Metamodell-Claims für alle relevanten Quelltypen vereinheitlichen:
  - direkter Neo4j-Read
  - lokaler Dump
  - Exportdateien
- Structured Claims als First-Class-Schema durchziehen:
  - `subject`, `predicate`, `operator`, `constraint`, `scope`, `assertion_status`, `source_authority`, `focus_value`
- Konfliktsemantik verschärfen:
  - Konflikt nur unterdrücken, wenn Operator, Scope, Menge und Bedeutung wirklich deckungsgleich sind
- Doku-/Konsensgewichtung finalisieren:
  - `explizite Wahrheit > bestätigte Entscheidung > SSOT-/Zieldoku > aktuelle Arbeitsdoku > AS-IS/Historie > Runtime-Beobachtung > Code-Ist > heuristische Inferenz`
- Für nicht direkt änderbare Quellen automatisch Jira-Änderungsvorschläge erzeugen:
  - `.puml`
  - generierte Dumps
  - externe Modellartefakte

### 3. Vollständige analytische Abdeckung des Zielbilds
**Status:** 60 %  
**Ziel:** keine relevanten Domänen auslassen.

- Domänen vollständig und explizit abdecken:
  - FIN-AI-Backend
  - Chunking/Mining
  - BSM-Agenten A3B/A4 und angrenzende Entscheider
  - alle Customer-DB-Write-Pfade
  - Guardrails/Policies
  - Metamodell- und Prozessdokumente
  - lokale `_docs`
  - Confluence-Zielbildseiten
  - fachlich relevante Config-/Frontend-/UI-Claims dort, wo sie Scope, Phase, Workflow oder Approval-Verhalten definieren
- Vollabdeckung heißt:
  - jede dieser Domänen muss Claims erzeugen können
  - in Root-Cause-Priorisierung vorkommen
  - im Gold-Set repräsentiert sein
- Repo-Coverage nicht mehr nur pfadbasiert, sondern symbol-/domainbasiert priorisieren:
  - Write-Sinks
  - Guardrail-Contracts
  - Phase-/Lifecycle-Definitionen
  - Persistenzadapter
  - Ticket-/Patch-Generatoren

### 4. Root-Cause-, Delta- und Retrieval-Qualität auf Endstand bringen
**Status:** 72 %  
**Ziel:** Kernprobleme zuerst, nicht Symptomhaufen.

- Causal-Graph als alleinige Primärbasis für:
  - Truth-Propagation
  - Delta-Rerender
  - Root-Cause-Bucket
  - Paketbildung
  - Retrieval-Ranking
- Symptom-Findings unter Primärursachen deduplizieren und als Supporting Evidence anhängen.
- Paketbildung final hartziehen:
  - Zielgröße `1-5`
  - Maximum `7`
  - automatische Aufspaltung entlang Root Cause / Write-Pfad / Entscheidungsthema
- Retrieval adaptiv machen:
  - mehr Budget für truth-kritische und kernursächliche Findings
  - Re-Ranking nach Quellengewicht, Delta-Typ, kausalem Abstand und Gold-Set-Severity
- Finding-Linking auf echte Ursache-Wirkung-Relationen statt bloße Scope-Überlappung umstellen.
- Delta-Neubewertung unter Last und über mehrere Läufe stabilisieren:
  - N / N+1 / N+2
  - transitive Cluster
  - unveränderte Pakete bleiben unangetastet

### 5. Action Layer und UI auf 100 % Zielbild ziehen
**Status:** 70 %  
**Ziel:** Ergebnisse nutzbar, reviewbar und vollständig steuerbar machen.

- Decision Packages, Truth Ledger, KI-Log, Approval Queue und Vollzugsledger gegen das Zielbild abgleichen und fehlende Felder schließen.
- Decision Packages verpflichtend ausstatten mit:
  - Primärursache
  - betroffene Wahrheiten
  - qualifizierte Adapter-/Driver-Symbole
  - Sink-/Schema-Ziele
  - exakte Änderungsanweisung
  - erlaubte Folgeaktion
- Jira-Briefs und Confluence-Patch-Previews auf denselben Kontextstandard heben.
- UI-Fokus:
  - Kernprobleme zuerst
  - offene Wahrheiten sichtbar
  - Delta-Auswirkungen nachvollziehbar
  - Approval-/Writeback-Folgen klar
- Keine neue Design-Breite; nur Arbeitsoberfläche vervollständigen, damit der Auditprozess ohne Nebenwerkzeuge nutzbar ist.

### 6. Produktionsnahe Integrationsverifikation
**Status:** 45 %  
**Ziel:** reale E2E-Verlässlichkeit statt nur technische Pfade.

- Echten OAuth-Consent im Auditor mit realen `granted_scopes` durchtesten.
- Echten Jira-Draft-End-to-End testen:
  - Approval
  - Payload
  - POST
  - Response
  - lokales Vollzugsledger
- Echten Confluence-Preview-/Patch-Pfad testen:
  - Zielseitenanker
  - Review-Markierung
  - Patch-Anwendung
  - Verifikation des Ergebnisses
- Fehlerfälle und Betriebsrealität beobachten:
  - 401/403
  - 429/Rate-Limits
  - Timeouts
  - Berechtigungsabweichungen
  - ungültige Redirect-/Scope-Konfiguration
- Testziele strikt von echten Produktivseiten trennen:
  - Testticket
  - Testseite
  - dokumentiertes Rollback

### 7. Betriebshärtung bis Production-Readiness
**Status:** 35 %  
**Ziel:** stabiler, wiederholbarer Dauerbetrieb.

- Größere Läufe auf realem FIN-AI-Datenbestand messen:
  - Laufzeit
  - Speicher
  - Wiederverwendung
  - Retrieval-Kosten
- Delta-/Inkrementalläufe unter Last prüfen.
- Idempotenz absichern:
  - Wiederanlauf nach Fehler
  - kein doppelter Vollzug
  - stabile Ledger-/Approval-Zustände
- Multi-Worker- und Lease-Recovery härten:
  - Heartbeats
  - Reclaim
  - stale-run recovery
- Logging, Tracing, Metriken und Failure-Handling auf Betriebsniveau bringen.
- Token-Lifecycle, Retry/Backoff und Cache-/Retention-Strategien finalisieren.

## Öffentliche Schnittstellen und Typen
- `ClaimRecord` wird auf ein echtes Assertion-/Authority-Schema gehoben.
- Neues `schema_truth_registry` mit Status:
  - `confirmed_ssot`
  - `provisional_target`
  - `observed_only`
  - `code_only_inference`
  - `rejected_target`
- Benchmark-Fallformat mit erwarteter Kategorie, Root Cause, Ranking und erlaubten Aktionen.
- Decision-Package- und Jira-/Confluence-Payloads verpflichtend mit qualifizierten Symbolen, Sink-/Schema-Details und Truth-/SSOT-Referenzen.

## Test- und Abnahmekriterien
### Für 100 % fachlich-analytische Nutzbarkeit im fokussierten Scope
- Gold-Set für den Write-Scope vollständig grün.
- Kritische FIN-AI-Kernprobleme werden reproduzierbar in den Top-Findings priorisiert.
- Kein kritischer False Negative im Write-Scope.
- Truth-Änderungen schärfen Zielbild und Delta korrekt über mehrere Läufe.

### Für 100 % Feature-Completeness
- Alle Domänen des Zielbilds sind in Claims, Findings, Packages und Action Layer abgedeckt.
- UI, Truth Ledger, Approval, Patch Preview und Jira Briefs entsprechen dem Zielbild funktional vollständig.
- Alle Gold-Set-Domänen grün, nicht nur der Write-Scope.

### Für 100 % Production-Readiness
- Echte OAuth-/Jira-/Confluence-E2E-Tests erfolgreich.
- Drei stabile Pilotläufe ohne kritischen Blocker.
- Last-/Delta-/Recovery-/Idempotenztests bestanden.
- Operative Runbooks und Rollbackpfade vorhanden.

## Fortschritt bis zum Endabschluss
- Nach Paket 1-4: fachlich-analytische Nutzbarkeit im fokussierten Scope von **82 % auf 100 %**
- Nach Paket 1-5: Feature-Completeness von **68 % auf 100 %**
- Nach Paket 6-7: Production-Readiness von **42 % auf 100 %**

## Feste Defaults
- Nichts aus der bereits erreichten Python-Write-Graph-Basis wird neu erfunden; diese Punkte gelten als abgeschlossen und dienen als Fundament.
- Vollabdeckung bedeutet gesamtes Zielbild, aber in Prioritätsreihenfolge:
  1. Write-Scope und Kernursachen
  2. BSM-/Metamodell-/Guardrail-/Doku-Semantik
  3. Action Layer und UI-Vervollständigung
  4. reale Integrationen
  5. Betriebs-Härtung
- „Schnell perfekt“ heißt hier: schnellstmögliche **belastbare** Lösung mit harten Gates, nicht Abkürzungen zulasten der Verlässlichkeit.
