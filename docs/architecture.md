# Architektur

Das kanonische Gesamtziel, die Produktvision und der Delivery-Pfad sind im
[Zielbild](./target-picture.md)
festgezogen. Diese Architekturdatei fokussiert auf die technische Form des Zielbilds.

## Ziel

FIN-AI Auditor ist ein eigenstaendiges System, das FIN-AI analysiert, aber nicht Teil seiner Runtime ist.

Das Tool soll:

- GitHub-Code analysieren
- Confluence-Doku analysieren und spaeter gezielt patchen
- Jira-Tickets als kontrolliertes Ziel fuer Codeaenderungs-Tickets erzeugen
- FIN-AI Metamodell- und Guardrail-Kontrakte einbeziehen
- daraus eine konsistente Soll/Ist-Auditierung aufbauen

## Architekturrichtlinien

- eigenes Repo
- eigener Deploy-Stack
- keine Runtime-Imports aus FIN-AI
- nur externe, stabile Schnittstellen
- evidence-first vor LLM-first
- section-anchored Doku-Patches statt Vollseiten-Rewrites
- externe Ressourcen bis zu einer expliziten User-Entscheidung strikt read-only
- einzige schreibende SSOT im Startstand ist die lokale Auditor-Datenbank

## Zielkomponenten

```text
Web UI
  -> API
      -> Audit Service
      -> Claim Extraction Pipeline
      -> Finding Engine
      -> Patch/Ticket Engine
      -> Persistence
  -> Worker
      -> Collector Jobs
      -> Claim Normalization Jobs
      -> Confluence Patch Preview Jobs
      -> Jira Draft Jobs
```

## Hauptdomänen

### 1. Collector Layer

Quellen:

- GitHub Repository Snapshot
- lokaler FIN-AI Repository Checkout unter dem GitHub-Verzeichnis als primaerer Repo-Zugriff
- Confluence Seiten und Anhange
- Jira als spaeteres Write-Ziel fuer Codeaenderungs-Tickets
- FIN-AI Metamodell direkt read-only aus Neo4j
- lokale Arbeitsdokumente

Der Collector liefert noch keine Entscheidungen, sondern nur belegbare Rohfakten.
Bis zu einer User-Freigabe werden diese Quellen ausschliesslich lesend angesprochen.

### 2. Claim Layer

Alle Quellen werden auf atomare Claims normalisiert, z. B.:

- Objekt `Statement` existiert
- Objekt `Statement` hat `review_status`
- Write-Pfad fuer `Statement` verlaeuft ueber Service X
- Doku behauptet Promotion-Regel Y
- Metamodell verweist auf Phase Z

Dieses Claim-Modell ist die zentrale SSOT des Auditors.

### 3. Finding Layer

Aus Claims entstehen Findings:

- `contradiction`
- `clarification_needed`
- `missing_definition`
- `stale_source`
- `implementation_drift`
- `read_write_gap`
- `traceability_gap`
- `ownership_gap`
- `policy_conflict`
- `terminology_collision`
- `low_confidence_review`
- `obsolete_documentation`
- `open_decision`

### 4. Action Layer

Aus Findings entstehen kontrollierte Aktionen:

- Rueckfrage an den User
- Confluence-Patch-Vorschlag
- Jira-Ticket-Draft
- Empfehlung fuer Code-Aenderung
- kleine Entscheidungs-Pakete mit `annehmen`, `ablehnen`, `spezifizieren`

Wichtig:

- Vorschlaege und Drafts bleiben lokal, bis der User eine explizite Entscheidung trifft
- kein externes Writeback auf Confluence, Jira oder andere Ressourcen ohne Freigabe

Die Zielarchitektur fuer Paketbildung, Truth-Ledger und Hybrid Retrieval ist in
[decision-packages-and-retrieval.md](/Users/martinwaelter/GitHub/FIN-AI-Auditor/docs/decision-packages-and-retrieval.md)
festgezogen.

## Technischer Startstand

Aktuell im Scaffold implementiert:

- FastAPI API
- lokale SQLite-SSOT fuer Audit-Runs, Quellensnapshots, Findings, Evidenzpositionen, Finding-Relationen, Claims, Truths, Entscheidungspakete, Entscheidungen und Approval-Requests
- produktive Worker-Pipeline fuer lokale Repo-Snapshots, Metamodell-Dumps, deterministische Claim-Extraktion, Retrieval-Indexierung und Finding-Generierung
- Claim-Extraktion besteht inzwischen aus AST-basierter Python-Strukturauswertung, TypeScript-/Frontend-Symbolerkennung, YAML-/Config-Key-Path-Auswertung sowie textuellem Fallback fuer Kommentare und nicht-strukturierte Stellen
- Confluence-Collector extrahiert nicht mehr nur flachen Seitentext, sondern wertet Storage-HTML und, wenn vorhanden, Atlas-Doc-Format mit strukturierten Heading-/Block-Pfaden aus
- semantische Widerspruchslogik bewertet Policy-, Lifecycle-, Scope- und Read/Write-Aussagen inzwischen facettenbasiert, damit textuelle Varianten nicht vorschnell als Konflikte eskalieren
- BSM-Prozessclaims werden tiefer normalisiert: Phase-/Frage-Referenzen, `phase_order`, `question_count` sowie Review-/Approval-Unterclaims werden kanonisch abgeleitet und fuer Konflikt- und Delta-Checks numerisch bzw. referenziell normalisiert
- zwischen Claims und Paketen liegt jetzt eine lokale semantische Graph-Schicht mit expliziten Knoten und Relationen fuer Objekte, Prozesse, Fragen, Policies, Dokumentsektionen und Codekomponenten
- die Semantik bildet inzwischen explizite Vertragsketten wie `phase -> question -> policy -> write_contract/read_contract` ab und nutzt dafuer BSM-Sektionskontext, Objektknoten und Contract-Knoten gemeinsam
- Entscheidungspakete und Empfehlungsgenerierung ziehen diese semantischen Relationen, Vertragsketten sowie Sektionshierarchien aus Confluence- und lokalen Dokuquellen gezielt als Kontext heran
- Text- und Confluence-Segmente werden mit Heading-Hierarchien, Ancestor-Kontext und tieferen Sektionspfaden indiziert, damit Retrieval und Delta-Neubewertung nicht nur auf flachen Textfenstern basieren
- der direkte Metamodell-Read deckt inzwischen neben `bsmPhase`/`bsmQuestion` auch Metaklassen, BSM-Funktionen und Label-Zusammenfassungen ab und fuehrt diese in den lokalen Dump und die Claim-Schicht ueber
- Claim-Deltas werden inkrementell als `exact`, `textual_only`, `semantic` oder `new_identity` klassifiziert; betroffene Scope-Cluster werden fuer die Neubewertung markiert
- geaenderte Confluence-Seiten werden claim-seitig jetzt abschnittsweise behandelt: unveraenderte Sektionen reuse-en bestehende Claims, geaenderte oder hinzugekommene Sektionen werden als fokussierte Abschnittsdokumente neu extrahiert, entfernte Sektionen loesen ihre alten Claims gezielt ab
- transitive Minimal-Neubewertung nutzt inzwischen lokale semantische Clusterbeziehungen zwischen Claims, damit nicht nur direkt geaenderte Scope-Keys, sondern auch eng verbundene Folgecluster gezielt nachgezogen werden
- Entscheidungspakete werden scope-basiert geclustert und uebernehmen Delta- und Retrieval-Signale des gesamten Clusters, nicht nur einzelner Findings
- der lokale Retrieval-Index besitzt jetzt eine SQLite-FTS5-Basis und mischt lexikale Treffer mit Struktur-, Delta- und Semantiksignalen zu einem hybriden Kontext-Ranking
- ein lokaler Dokument-Cache erlaubt erste inkrementelle Wiederverwendung unveraenderter Repo- und Confluence-Inhalte ueber Content-Hash-, Revisions- und Datei-Stat-Signale
- Confluence wird lokal nicht als zweites Wiki gespiegelt, sondern als versionierter `analysis cache` mit Seiten-Registry, Retention-Regeln, Restriktions-/Sensitivitaetsstatus und Abschnitts-Deltas gefuehrt
- inkrementelle Claim-/Finding-Neubewertung kann fuer unveraenderte Quellen bereits lokale Claims und Findings wiederverwenden und fokussiert Regeneration auf betroffene Scope-Cluster
- React/Vite Frontend fuer Run-Anlage, atomare Entscheidungspakete, Approval-Queue, Claim-/Truth-Ledger und lokalen Writeback-Vollzug
- lokaler Freigabefluss fuer spaetere Writebacks inklusive Jira-AI-Coding-Briefs, section-anchored Confluence-Patch-Preview und lokalem Vollzugsledger
- entkoppelte LiteLLM-Basis im Auditor, die FIN-AI Slot-Konfigurationen lesen und fuer Chat/Embeddings aufloesen kann
- read-only GitHub-/Local-Repo-Collector, produktiver Confluence-Collector ueber Atlassian-3LO-Access-Token und read-only Metamodell-Dump-Collector
- lokaler Atlassian-3LO-Consent-Flow mit eigener Auditor-Callback-URI, lokaler Token-SSOT und Verifikationsroute fuer echte Confluence-Live-Reads
- Bootstrap und UI unterscheiden technisch sauber zwischen OAuth-Readiness, aktuellem Confluence-Live-Read und echtem Jira-Writeback-Scope
- Scope-Enforcement fuer externen Jira-Writeback basiert jetzt auf den dem aktuellen Token tatsaechlich gewaehrten Scopes, nicht auf bloss konfigurierten Wunsch-Scopes
- externer Confluence-Writeback ist technisch vorhanden und fuehrt nach Approval section-anchored Review-Patches ueber die Confluence API aus; produktive Verifikation gegen echte FINAI-Seiten steht noch aus
- LLM-gestuetzte Empfehlungsschaerfung mit kontrolliertem Fallback auf deterministische Empfehlungen
- lokaler Retrieval-Index fuer Segment-, Claim- und Kontextsuche mit optionalen Embeddings
- Worker und Pipeline loggen jetzt strukturiert im JSON-Format; Runtime-Spans und Metriken werden lokal in SQLite persistiert und ueber Health/Bootstrap zusammengefasst
- Confluence-Reads/-Writes besitzen erste Retry-/Rate-Limit-Backoff-Pfade; Storage-/ADF-Extraktion behandelt neben normalen Textblöcken jetzt auch Tabellen, Makros, Attachments, Status- und Card-Knoten strukturierter
- Restriktions- und Sensitivitaetsbewertung fuer Confluence beruht jetzt konservativer auf expliziten Restriktionen, Labels und Properties; `operations` dienen nur noch als Access-Hinweis, nicht mehr als Beweis fuer fehlende Restriktionen
- Run-Claims werden atomarer ueber eine Lease-/Heartbeat-Grundlage abgesichert; owner-aware Heartbeats verhindern fremde Lease-Verlaengerungen und stale Runs werden fuer Reclaim sichtbar gemacht

Noch offen:

- noch tiefere semantische Matching-, Delta- und Patch-Engine ueber die jetzt abgedeckte Objekt-, Policy-, Lifecycle-, Vertragsketten-, Sektions-, Config-, Frontend- und BSM-Prozesssemantik hinaus
- voll inkrementelle Reanalyse geaenderter Quellbereiche; die source-lokale Claim-/Finding-Wiederverwendung steht, transitive Minimal-Neubewertung ueber alle abhaengigen Cluster ist aber noch nicht komplett ausgereift
- breitere strukturierte Extraktion aus weiteren Guardrail-Vertraegen, zusaetzlichen Meta-Objekten und komplexeren verbleibenden Confluence-Sonderfaellen
- produktive Betriebs-Haertung fuer tieferes Fehlertracing, Rate-Limits, Retries, Token-Lifecycle und Multi-Worker-Lease-Recovery
- produktive End-to-End-Verifikation des vorhandenen externen Writebacks auf Confluence/Jira unter realem OAuth-Consent
- produktive Registrierung der lokalen Auditor-Redirect-URI in der Atlassian-App

## Warum kein FIN-AI-Unterprojekt

Das Tool ist fachlich verwandt, aber operativ ein anderes Produkt.

Die Trennung ist hier gewollt:

- kein Mitdeployen
- keine Build-Kopplung
- kein Risiko, FIN-AI Runtime mit Audit-Logik aufzublasen
- klare Governance-Grenze
