# Production-Readiness Runbook

Dieses Runbook beschreibt die kontrollierte Abschlussstrecke fuer den produktionsnahen Auditor-Betrieb.
Es geht bewusst nicht um vollautomatischen Dauerbetrieb, sondern um einen belastbaren, wiederholbaren Pilot- und Go-Live-Nachweis.

## Ziel

Der Auditor soll nachweisen, dass er:

- Confluence-, Code-, lokale Doku- und Metamodell-Aussagen belastbar gegeneinanderstellt
- daraus atomare Fakten, Entscheidungspakete und Folgeaktionen ableitet
- Jira- und Confluence-Writebacks nur nach expliziter Approval-Entscheidung ausfuehrt
- externe Vollzuege lokal nachvollziehbar im Ledger und im Fakten-/Paketkontext verbucht

## Voraussetzungen

- lokaler Auditor-Token mit mindestens:
  - `read:page:confluence`
  - `write:page:confluence`
  - `read:space:confluence`
  - `write:jira-work`
- funktionsfaehige lokale Callback-URI:
  - `http://localhost:8088/api/ingestion/atlassian/auth/callback`
- Jira-Zielprojekt:
  - `FINAI`
- kontrollierte Confluence-Testseite:
  - Space `FP`
  - Seite `2654426`
  - Titel `FIN-AI Testing`

## Wiederholbarer Live-Smoke

Der Auditor enthaelt dafuer jetzt einen reproduzierbaren Smoke-Runner:

```bash
cd /Users/martinwaelter/GitHub/FIN-AI-Auditor
uv run python scripts/live_writeback_smoke.py --reset-runs
```

Optional:

```bash
uv run python scripts/live_writeback_smoke.py --skip-confluence
uv run python scripts/live_writeback_smoke.py --skip-jira
```

Der Runner:

- legt einen kontrollierten Auditor-Run an
- vervollstaendigt ihn mit Demo-Findings
- verankert den driftenden Demo-Fall auf der echten Confluence-Testseite
- erzeugt einen Approval-Request fuer Jira
- genehmigt ihn lokal
- fuehrt den externen Jira-Writeback aus
- erzeugt einen Approval-Request fuer Confluence
- genehmigt ihn lokal
- fuehrt den externen Confluence-Patch aus
- gibt ein JSON-Ergebnis mit Run-ID, Jira-Issue und Confluence-Revision aus

## Bereits real verifiziert

Am **15. Maerz 2026** wurde die produktionsnahe E2E-Strecke lokal erfolgreich gegen echte Testziele verifiziert:

- Jira-Ticket erstellt:
  - `FINAI-361`
- Confluence-Seite aktualisiert:
  - `FIN-AI Testing`
  - `page_id = 2654426`
  - neue Revision `8`

Wichtig:

- Jira musste dabei nicht auf einem fest verdrahteten `Story`-Issue-Type laufen. Der Connector loest jetzt projektfaehig auf einen erlaubten Typ auf und fiel im echten Projekt korrekt auf `Task` zurueck.
- Der Confluence-Pfad lief ueber den echten Approval- und Patch-Mechanismus des Auditors, nicht ueber einen separaten Direktaufruf.

## Erwartete Erfolgsmerkmale

- `approval_requests[*].status == executed`
- `implemented_changes` enthaelt:
  - `jira_ticket_created`
  - `confluence_page_updated`
- `writeback_verification.verified == true`
- Jira hat eine echte Issue-URL
- Confluence liefert eine neue Seitenrevision
- Analysis-Log enthaelt:
  - `Jira-Writeback ausgefuehrt`
  - `Confluence-Writeback ausgefuehrt`

## Rollback / Bereinigung

- Jira:
  - Testticket im Projekt `FINAI` manuell schliessen, labeln oder loeschen gemaess Projektregeln
- Confluence:
  - auf der Testseite nur im Testbereich arbeiten
  - Review-Block bei Bedarf manuell entfernen oder sauber als Smoke-Nachweis stehen lassen

## Verbleibende Produktionsluecken

Der fachlich-funktionale Auditor ist abgeschlossen. Weiter offen fuer einen haerteten Dauerbetrieb sind vor allem:

- Lasttests auf groesseren FIN-AI-Laeufen
- Delta-/Inkrementallaeufe unter Last
- Wiederanlauf- und Lease-Recovery unter echten Fehlern
- weitergehende Metriken, Alarmierung und Failure-Triage
- Pilotbetrieb mit mehreren aufeinanderfolgenden Laeufen und dokumentierten Go-Live-Gates

## Go-Live-Gates

Vor einer produktiven Freigabe sollten mindestens diese Gates explizit abgehakt sein:

1. Gold-Set und Delta-Gate gruen
2. Live-Smoke gegen Jira und Confluence erfolgreich
3. keine offenen Scope-/OAuth-Blocker
4. Recovery-/Lease-Verhalten unter simulierten Fehlern verifiziert
5. Pilotlaufserie dokumentiert und fachlich akzeptiert
