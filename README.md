# FIN-AI Auditor

FIN-AI Auditor ist ein eigenstaendiges Governance- und Audit-Tool fuer die Analyse von Soll/Ist-Abweichungen zwischen:

- GitHub-Code
- Confluence-Dokumentation
- Jira-Tickets als Ziel fuer spaetere Codeaenderungs-Tickets
- FIN-AI Metamodell- und Guardrail-Vertraegen

Das Repo ist bewusst **separat von FIN-AI** gehalten. Es wird nicht mit FIN-AI mitdeployt, nicht in FIN-AI verdrahtet und importiert keine FIN-AI-Runtime-Module.

Bis zu einer expliziten User-Entscheidung arbeitet der Auditor gegen externe Systeme strikt **read-only**. Schreibend verwendet werden darf ausschliesslich die eigene lokale SQLite-Datenbank des Auditors.

## Zielbild

FIN-AI Auditor soll fachliche und technische Aussagen in ein kanonisches Claim-Modell ueberfuehren und daraus:

- Widersprueche
- fehlende Definitionen
- nicht dokumentierte Lese-/Schreibpfade
- veraltete Aussagen
- unklare Entscheidungen

sichtbar machen. Darauf aufbauend soll das Tool Rueckfragen orchestrieren, Confluence-Patches vorbereiten und Jira-Tickets mit konkreten Umsetzungs-Prompts erzeugen.

## Repo-Status

Aktueller Stand dieses Scaffolds:

- FastAPI-Basis fuer Audit-Runs
- kleine lokale SQLite-DB als Audit-SSOT fuer Runs, Findings, Positionen und Delta-Metadaten
- produktive Worker-Pipeline fuer lokales FIN-AI-Repo, direkten Metamodell-Dump und read-only Confluence-Read ueber Atlassian-3LO-Access-Token
- lokaler Atlassian-3LO-Consent-Flow mit eigener Callback-URI des Auditors, lokaler Token-Ablage und read-only Confluence-Live-Read-Verifikation
- robusterer produktiver Confluence-Live-Read mit sauberem Fallback auf den direkten Site-Endpunkt und klaren Analysehinweisen bei 401/403/429 oder fehlender Cloud-Ressource
- Confluence-Extraktion wertet jetzt sowohl Storage-HTML als auch, wenn verfuegbar, Atlas-Doc-Format strukturiert aus und fuehrt Heading-/Block-Kontext lokal mit
- React/Vite-Frontend fuer Start, Uebersicht und Detailansicht von Audit-Runs
- laufender Analysefortschritt mit Phasen wie Metamodell-Pruefung, FIN-AI Code-Pruefung, Confluence-Pruefung, Delta-Abgleich und LLM-Empfehlungen
- atomare Entscheidungspakete mit Problemelementen, Empfehlungen und lokalen Paketentscheidungen
- lokale SQLite-Tabellen fuer Claim-Index, Truth-Ledger, Entscheidungspakete, Entscheidungsprotokolle und Approval-Requests
- lokale Retrieval-Indexierung mit Segmenten, Claim-Verknuepfungen, Delta-Markierung und optionalen Embeddings
- KI-Statuslog fuer lokale User-Kommentare mit abgeleiteten Wahrheiten, Scope-Folgen und Neugewichtungs-Hinweisen
- lokaler Freigabefluss fuer spaetere Writebacks: anfordern, genehmigen, ablehnen, lokal als umgesetzt verbuchen
- lokales Ledger fuer umgesetzte Confluence-Updates und erstellte Jira-Tickets
- Confluence-Patch-Preview mit section-anchored Review-Operationen, farblich markierten Review-Snippets und Approval-Vorbereitung im lokalen Ledger
- technischer Confluence-Writeback-Pfad hinter expliziter Freigabe; der Auditor schreibt erst nach Approval und nur mit gueltigem `write:page:confluence`-Scope extern
- Jira-AI-Coding-Briefs mit Problem, Grund, Korrekturmassnahmen, Zielbild, Abnahmekriterien, Implikationen, betroffenen Teilen, Evidenz und validierbarem Prompt-Kontext
- kontrollierter Jira-Writeback-Pfad hinter expliziter Freigabe; ohne `write:jira-work` blockiert der Auditor den externen Ticket-POST hart
- Auditor-eigene LiteLLM-Basis, die FIN-AI-kompatible `FINAI_LLM_<slot>_*`-Konfigurationen lesen kann
- produktive Claim-Extraktion, deterministische Finding-Generierung und Delta-Markierung auf Snapshot- und Claim-Ebene
- mehrstufige Claim-Extraktion: Regex-Fallback plus AST-basierte Python-Erkennung fuer Klassen, Funktionen und Router-Handler
- breitere Claim-Extraktion fuer TypeScript-/Frontend-Dateien sowie YAML-/Config-Vertraege mit strukturierter Key-Path- und Symbol-Erkennung
- geschaerfte Claim-Heuristiken fuer konkrete FIN-AI-Objekte wie `Statement`, `FINAI_Job`, `FINAI_Project`, `BSM_Phase` und Policy-Vertraege
- semantische Widerspruchslogik fuer Policy-, Lifecycle-, Scope- und Read/Write-Aussagen, damit textuelle Varianten nicht vorschnell als fachliche Konflikte gewertet werden
- tiefere Prozesssemantik fuer BSM-Claims mit Phase-/Frage-Referenzen, `phase_order`-, `question_count`- und Review-/Approval-Unterclaims sowie kanonischer Zahlennormalisierung
- lokale semantische Graph-Schicht mit expliziten Knoten und Relationen fuer Objekt-, Prozess-, Policy-, Sektions- und Codekontexte
- explizite Vertragsketten in der Semantik, z. B. `phase -> question -> policy -> write_contract/read_contract`, inklusive Kontextherleitung aus BSM-Heading-Hierarchien
- Entscheidungspakete und Empfehlungen nutzen jetzt neben Findings auch semantische Knoten, Relationen, Vertragsketten und Sektionspfade als Kontext
- Confluence- und Doku-Sektionen werden ueber Heading-Hierarchien und Ancestor-Kontext tiefer in Claim- und Retrieval-Kontext ueberfuehrt
- direkter Metamodell-Read ueber `bsmPhase`/`bsmQuestion` hinaus: Metaklassen, BSM-Funktionen und Label-Zusammenfassungen werden mit in den lokalen Dump und die Claim-Extraktion aufgenommen
- inkrementelle Delta-Neubewertung mit Unterscheidung zwischen `exact`, `textual_only`, `semantic` und `new_identity` auf Claim-Ebene
- Scope-basierte Entscheidungspakete, die mehrere Problemelemente, Delta-Signale und Retrieval-Kontexte eines fachlichen Clusters zusammenfassen
- Delta- und Scope-Notizen werden im Statuslog bevorzugt sichtbar gehalten, damit der User betroffene Cluster und semantische Aenderungen nachvollziehen kann
- lokaler SQLite-FTS5-Index fuer Retrieval-Segmente sowie hybrides Kontext-Ranking aus Struktur-, Lexik- und Semantiksignalen
- lokaler Dokument-Cache fuer Repo- und Confluence-Quellen, damit unveraenderte Inhalte ueber Snapshot- und Revisionssignale inkrementell wiederverwendet werden koennen
- Confluence wird lokal explizit als `analysis cache` gehalten: kein Vollmirror, sondern versionierter Arbeitscache mit Seiten-Registry, Retention-Regeln, Sensitivitaets-/Restriktionsmetadaten und Abschnitts-Deltas
- inkrementelle Claim-/Finding-Neubewertung nutzt jetzt fuer unveraenderte Quellen wiederverwendbare lokale Claims und fokussiert Regeneration auf betroffene Scope-Cluster statt blind auf den gesamten Vorgaengerlauf
- geaenderte Confluence-Seiten koennen claim-seitig jetzt abschnittsweise regeneriert werden: unveraenderte Sektionen reuse-en lokale Claims, geaenderte/neu hinzugekommene Sektionen werden gezielt neu extrahiert, entfernte Sektionen verlieren ihre alten Claims ohne Vollseiten-Reparse
- transitive Minimal-Neubewertung ist fuer semantisch verbundene Scope-Cluster jetzt enger: geaenderte Cluster ziehen ueber lokale semantische Clusterbeziehungen nur noch ihre fachlich angrenzenden Folgebereiche nach
- atomareres Run-Claiming mit Lease-/Heartbeat-Grundlage fuer mehrere Worker statt nur losem Status-Polling
- owner-aware Lease-Heartbeats verhindern jetzt, dass fremde Worker einen aktiven Lauf unbemerkt weiterziehen oder leeren
- strukturierte JSON-Runtime-Logs fuer Worker und Pipeline, persistierte Runtime-Spans/Metriken in SQLite sowie Retry-/Rate-Limit-Backoff in den Confluence-HTTP-Pfaden
- Health- und Bootstrap-Endpunkte liefern jetzt lokale Observability- und stale-run-Recovery-Zusammenfassungen aus
- Health- und Bootstrap-Endpunkte liefern ausserdem den aktuellen Stand des lokalen Confluence-Analysis-Caches inklusive Retention-Policy und Seiten-Registry
- Confluence-Extraktion erkennt neben Heading-/Block-Pfaden jetzt auch Tabellenzeilen, Makros, Attachments, Statusknoten, Cards und weitere ADF-Sonderknoten strukturierter
- Restriktions- und Sensitivitaetsmetadaten aus Confluence werden jetzt konservativer und belastbarer hergeleitet: explizite Restriktionen, Properties und Labels werden ausgewertet, reine `operations`-Antworten gelten dagegen nicht mehr faelschlich als Beleg fuer fehlende Restriktionen
- Bootstrap und UI unterscheiden jetzt klar zwischen konfigurierter Atlassian-Integration, lokal betriebsbereitem OAuth-Flow, aktuellem Confluence-Live-Read und echtem Jira-Write-Scope
- Architektur- und Produktdoku fuer die naechsten Ausbauschritte
- Bootstrap-Defaults fuer den lokalen FIN-AI-Checkout unter `/Users/martinwaelter/GitHub/FIN-AI`
- fixes Analyse- und Zielprofil:
  - Confluence: `https://fin-ai.atlassian.net/wiki/spaces/FP/overview`
  - Jira Board als Ticket-Ziel: `https://finius.atlassian.net/jira/software/projects/FINAI/boards/67`
- Metamodell wird pro Lauf immer lesend als aktueller lokaler Dump unter `data/metamodel/current_dump.json` behandelt
- externe Ressourcen bleiben bis zur User-Freigabe read-only; Vorschlaege, Patches und Ticket-Drafts bleiben lokal

Noch **nicht** implementiert:

- persistente Produktionsdatenbank
- produktiv verifizierter externer Confluence-Writeback gegen echte FINAI-Seiten unter realem OAuth-Consent
- produktiv verifizierte externe Jira-Ticket-Erstellung gegen das echte FINAI-Projekt unter realem OAuth-Consent
- strukturierte Tiefenextraktion fuer weitere Quellen und Vertraege, z. B. komplexere Confluence-Makros, tiefere Storage-/ADF-Sonderfaelle, weitere Meta-Objekte, Guardrail-Vertraege und breitere Frontend-/Config-Muster
- voll inkrementeller Betrieb mit partieller Reanalyse geaenderter Quellbereiche; source-lokale Claim-/Finding-Wiederverwendung und erste section-level Claim-Regeneration stehen, die transitive Neubewertung ueber alle betroffenen Cluster ist aber noch nicht komplett minimal
- tieferes Hybrid-Retrieval-Ranking und robustere semantische Clusterbildung ueber mehrere Quellen, Prozessketten und transitive Konflikte hinweg
- betriebliche Haertung fuer produktiven Dauerbetrieb: weitergehendes Fehlertracing, Token-Lifecycle-Handling und staerkere Multi-Worker-Lease-/Recovery-Strategien

Aktuelle technische Hinweise:

- Confluence-Read ist implementiert und nutzt mit gueltigem Access Token den Atlassian-3LO-Pfad ueber `accessible-resources`; der Auditor kann den noetigen 3LO-Consent jetzt lokal selbst starten, braucht dafuer aber einen einmaligen Browser-Login.
- Fuer den separaten Auditor ist die kanonische lokale Callback-URI `http://localhost:8088/api/ingestion/atlassian/auth/callback`. Wenn in Atlassian noch die alte FIN-AI-Callback-URI registriert ist, muss die App-Konfiguration angepasst werden.
- Fuer Scope-Pruefungen auf Jira-Writeback wird jetzt nur noch der tatsaechlich dem aktuellen Token gewaehrte Scope-Satz verwendet. Konfigurierte Wunsch-Scopes allein reichen nicht mehr fuer einen externen Writeback.
- Metamodell-Read nutzt bei gesetzter `FINAI_META_SOURCE=DIRECT` eine direkte read-only Neo4j-Verbindung. Wenn der direkte Zugriff fehlschlaegt, wird kontrolliert auf den letzten lokalen Dump unter `data/metamodel/current_dump.json` zurueckgefallen.
- Die LiteLLM-Empfehlungsschicht ist produktiv verdrahtet und faellt bei Providerfehlern sauber auf die deterministischen Empfehlungen zurueck.
- Jira-Tickets werden lokal bereits als strukturierte Issue-Payloads mit ADF-Beschreibung vorbereitet und im Approval-Flow vorgelagert, auch wenn der externe Writeback noch nicht aktiviert ist.
- Der externe Jira-Writeback kann jetzt nach Approval technisch ausgefuehrt werden, bleibt aber ohne frischen OAuth-Consent mit wirklich gewaehrtem `write:jira-work` blockiert.
- Der externe Confluence-Writeback nutzt eine section-anchored Review-Patch-Engine mit farbigen Review-Markierungen. Ohne frischen OAuth-Consent mit `write:page:confluence`, ohne echten Zielseitenanker und ohne Live-Verifikation gegen reale FINAI-Seiten bleibt der externe Vollzug blockiert.

Die lokale Datenbank liegt standardmaessig unter `data/fin_ai_auditor.db`.

## Struktur

```text
FIN-AI-Auditor/
  docs/                   # Architektur, Scope, Datenmodell, Roadmap
  src/fin_ai_auditor/     # API, Domain, Services, Worker
  tests/                  # erste API-Tests
  web/                    # separates React/Vite-Frontend
  data/                   # lokale Entwicklungsdaten (nicht committen)
```

## Schnellstart

### 1. Python-Umgebung

```bash
cd /Users/martinwaelter/GitHub/FIN-AI-Auditor
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. API starten

```bash
source .venv/bin/activate
python -m fin_ai_auditor.main
```

API-Health:

```bash
curl http://127.0.0.1:8088/api/health
```

### 3. Worker einmalig laufen lassen

```bash
source .venv/bin/activate
python -m fin_ai_auditor.worker.main --once
```

### 4. Frontend starten

```bash
cd /Users/martinwaelter/GitHub/FIN-AI-Auditor/web
npm install
npm run dev
```

Das Frontend erwartet die API standardmaessig unter `http://127.0.0.1:8088`.

Der Auditor ist so vorbereitet, dass FIN-AI primaer ueber den lokalen Repo-Pfad im GitHub-Verzeichnis adressiert wird.
Confluence wird als feste Analysequelle behandelt, Jira nur als festes Ticket-Ziel fuer spaetere Codeaenderungs-Tickets, und das Metamodell ist im Laufprofil immer aktiv.
Bis zu einer User-Entscheidung sind alle externen Zugriffe lesend. Schreibend genutzt wird nur `data/fin_ai_auditor.db`.
Wenn `FINAI_LLM_<slot>_*`-Variablen im Auditor-Kontext vorhanden sind, kann die portierte LiteLLM-Schicht dieselben Slots fuer Chat- und Embedding-Aufrufe verwenden.

## Naechste sinnvolle Umsetzungsschritte

1. echten OAuth-Consent im Auditor mit realen `granted_scopes` durchlaufen und den Live-Status technisch verifizieren
2. den vorhandenen Jira-Execution-Pfad gegen ein echtes Testticket im FINAI-Projekt pruefen
3. den vorhandenen Confluence-Execution-Pfad gegen eine echte FINAI-Testseite pruefen
4. die Claim-Extraktion und Metamodell-Semantik auf weitere Fachobjekte, Guardrails und ADF-/Storage-Strukturen ausbauen
5. inkrementelle Reanalyse, Hybrid-Retrieval und Betriebs-Haertung fuer groessere Dauerlaeufe weiter vertiefen

## Doku-Einstieg

- [Zielbild](./docs/target-picture.md)
- [Architektur](./docs/architecture.md)
- [Produktscope](./docs/product-scope.md)
- [Datenmodell](./docs/data-model.md)
- [Entscheidungs-Pakete und Retrieval](./docs/decision-packages-and-retrieval.md)
- [Delta und Aufloesungsstrategie](./docs/delta-sync-and-resolution.md)
- [Roadmap](./docs/roadmap.md)
