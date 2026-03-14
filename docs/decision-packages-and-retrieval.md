# Entscheidungs-Pakete, Truth-Ledger und Retrieval-Architektur

## Ziel

Nach jedem Audit-Lauf soll der User **kleine, bearbeitbare Entscheidungs-Pakete** sehen statt einer flachen Finding-Liste. Diese Pakete muessen:

- fachlich zusammenhaengende Probleme gruppieren
- die relevanten Aussagen und Quellen sichtbar machen
- eine konkrete Empfehlung geben
- eine Entscheidung erlauben: `annehmen`, `ablehnen`, `spezifizieren`

Wichtig:

- `spezifizieren` ist keine freie Notiz, sondern eine neue **atomare Wahrheit** im Auditor
- diese Wahrheit beeinflusst kuenftige Re-Audits, Claim-Generierung und Paketbildung
- externe Systeme bleiben bis zur User-Freigabe read-only; gespeichert wird nur lokal im Auditor

## 1. Zielstruktur in der UI

Der User sieht nach einem Lauf zuerst **Bewertungskategorien** und darin **atomare Problemelemente**.

### Empfohlene Kategorien

- `Widersprueche`
  - dokumentierte Aussage A steht gegen Code, Metamodell oder andere Doku-Aussage B
- `Klarstellungen / fehlende Informationen`
  - Definition fehlt, Scope ist unklar, Lifecycle ist nicht vollstaendig
- `Implementierungsdrift`
  - Code verhaelt sich anders als dokumentiert oder modelliert
- `Read-/Write-Luecken`
  - Lese- oder Schreibverhalten ist nur auf einer Seite beschrieben oder umgesetzt
- `Terminologiekonflikte`
  - verschiedene Begriffe fuer dasselbe Objekt oder derselbe Begriff fuer verschiedene Dinge
- `Ownership-/Verantwortungsluecken`
  - unklar, welche Seite, welches Objekt oder welches Team kanonisch ist
- `Traceability-Luecken`
  - Aussage hat keine saubere Rueckbindung auf Code, Doku, Metamodell oder Entscheidung
- `Policy- oder Guardrail-Konflikte`
  - Dokument, Code oder Prozess verletzt den festgelegten Architektur- oder Governance-Vertrag
- `Veraltete Quellen`
  - Quelle ist fachlich ueberholt oder wird von juengeren, widersprechenden Quellen ueberholt
- `Low-Confidence Review`
  - der Auditor findet etwas Auffaelliges, aber die Evidenzlage ist noch nicht stark genug

Nicht jede Kategorie muss in jedem Lauf sichtbar sein. Die UI zeigt nur Kategorien mit offenen Elementen.

## 2. Aufbau eines atomaren Problemelements

Jedes Problemelement braucht eine klar determinierte Struktur:

- `problem_id`
- `package_id`
- `category`
- `severity`
- `scope`
  - Objekt
  - Property
  - Prozessschritt
  - Seite
  - Service
- `short_explanation`
- `evidence`
  - Quellstellen mit Originalposition
  - relevante Zitate/Fragmente nur ausschnittsweise
- `recommendation`
- `confidence`
- `affected_claim_ids`
- `affected_truth_ids`

### User-Aktionen

- `annehmen`
  - Empfehlung wird als bestaetigte Aufloesung uebernommen
- `ablehnen`
  - Empfehlung wird verworfen; das Problem bleibt mit neuer Begruendung offen oder geht auf `dismissed`
- `spezifizieren`
  - der User gibt eine neue fachliche Wahrheit oder Randbedingung ein

## 3. Truth-Ledger statt Freitext-Notizen

Die wichtigste Regel ist:

- User-Spezifizierungen werden nicht als lose Kommentare gespeichert
- sie werden als **kanonische Wahrheitseintraege** persistiert

### Beispiel

Statt:

- "Bitte beachten, dass Statement X nur im Review-Status schreibbar ist"

speichert der Auditor:

- `subject = Statement`
- `predicate = write_allowed_when`
- `value = review_status in ['draft', 'in_review']`
- `scope = FINAI global`
- `source = user_specification`
- `status = active`

### Empfohlene Truth-Felder

- `truth_id`
- `canonical_key`
- `subject_kind`
- `subject_key`
- `predicate`
- `normalized_value`
- `scope_kind`
- `scope_key`
- `truth_status`
  - `active`
  - `superseded`
  - `rejected`
- `source_kind`
  - `user_specification`
  - `user_acceptance`
  - `system_inference`
- `created_from_problem_id`
- `supersedes_truth_id`
- `valid_from_snapshot_id`
- `metadata_json`

## 4. Entscheidungs-Pakete

Ein Entscheidungs-Paket gruppiert mehrere atomare Problemelemente, wenn sie denselben fachlichen Kern beruehren.

### Paketbildung

Ein Paket darf nur gebildet werden, wenn mindestens einer der folgenden Anker uebereinstimmt:

- gleiches Objekt oder gleiches `subject_key`
- gleiche Property
- gleicher Prozessschritt
- gleiche Doku-Seite plus gleicher Service
- gleicher Widerspruchscluster

### Paket-Groesse

Ein Paket soll klein bleiben:

- ideal: `1-5` Problemelemente
- hartes Ziel: maximal `7`

Wenn mehr Elemente zusammenfallen, wird das Cluster weiter aufgeteilt, damit der User nicht wieder einen unstrukturieren Block vor sich hat.

### Paketfelder

- `package_id`
- `title`
- `category`
- `severity_summary`
- `scope_summary`
- `problem_ids`
- `decision_state`
- `decision_required`
- `rerender_required_after_decision`

## 5. Wirkung einer User-Entscheidung

Eine Entscheidung wirkt nicht nur auf das einzelne Element, sondern auf den gesamten Bewertungsraum.

### Deshalb braucht der Auditor drei Ledger

- `Claim Ledger`
  - extrahierte Aussagen aus Code, Metamodell und Doku
- `Truth Ledger`
  - bestaetigte oder spezifizierte User-Wahrheiten
- `Resolution Ledger`
  - Annahme, Ablehnung oder Ersetzung von Empfehlungen

### Neugenerierung nach Truth-Aenderung

Wenn eine atomare Wahrheit neu angelegt oder geaendert wird, muessen nicht alle Inhalte neu erzeugt werden. Stattdessen:

1. betroffene Claims finden
2. betroffene Problem-Cluster finden
3. nur diese Pakete neu generieren
4. unveraenderte Pakete stehen lassen

## 6. Indizierung und Retrieval: nicht immer alles neu lesen

Der Auditor darf nicht bei jedem Lauf das komplette Repo und alle Confluence-Inhalte wieder voll in den Kontext laden.

Deshalb braucht er einen **mehrschichtigen lokalen Index**.

### 6.1 Source Registry

Pro Quelle wird lokal festgehalten:

- stabiler Quellschluessel
- letzte Revision
- letzter `content_hash`
- letzter erfolgreicher Import
- letzter Claim-Stand
- letzte bekannte Paket-Beteiligung

### 6.2 Segment Index

Jede Quelle wird in **stabile Segmente** zerlegt:

- Code
  - Datei
  - Symbol
  - Funktion
  - Klasse
  - Line-Range
- Confluence
  - Seite
  - Heading Path
  - Block Index
  - ADF-/Storage-Anchor
- Metamodell
  - Node
  - Relationship
  - Property
  - Dump-Anchor
- lokale Doku
  - Datei
  - Heading Path
  - Line-Range

Jedes Segment bekommt:

- `segment_id`
- `source_snapshot_id`
- `anchor`
- `text_hash`
- `structure_hash`
- `semantic_fingerprint`

### 6.3 Claim Index

Aus Segmenten werden Claims erzeugt. Claims werden deterministisch normalisiert und ueber Fingerprints wiedererkannt.

Claim-Fingerprint basiert mindestens auf:

- `subject_kind`
- `subject_key`
- `predicate`
- `normalized_value`
- `scope`

### 6.4 Truth Index

User-Wahrheiten werden wie priorisierte Claims behandelt. Bei Konflikten gilt:

1. aktive User-Wahrheit
2. bestaetigte Entscheidung
3. Metamodell
4. dokumentierte Aussage
5. implementierte Aussage
6. heuristische LLM-Inferenz

Diese Priorisierung ist wichtig, damit ein neues Re-Audit eine explizite User-Klarstellung nicht wieder mit statistischen Aussagen ueberfaehrt.

### 6.5 Hybrid Retrieval

Der Auditor sollte lokal drei Retrieval-Arten kombinieren:

- `strukturell`
  - Objekt, Symbol, Section Path, Metaclass, Property
- `lexikalisch`
  - FTS/BM25 ueber normalisierte Segmente
- `semantisch`
  - Embeddings fuer Segmente, Claims und Paketfragen

Das Ziel ist **hybrides Candidate Retrieval + lokales Reranking**, nicht ein blindes Volltext-Prompting.

## 7. Empfohlene lokale Index-Strategie

### Phase A: deterministisch und billig

Pflicht:

- Source Registry
- Snapshot Chain
- Segment Index
- Claim Index
- SQLite FTS5 fuer Segmenttexte und Claim-Texte

Das liefert schon einen grossen Teil des Nutzens ohne Embeddings.

### Phase B: semantische Beschleunigung

Danach lokal ergaenzen:

- Embeddings fuer Segmente
- Embeddings fuer Claims
- Embeddings fuer User-Spezifizierungen
- Hybrid-Ranking: `lexical score + structural score + vector score`

### Warum nicht alles jedes Mal neu embedden?

Nur neu embeddet werden:

- neue oder geaenderte Segmente
- Segmente, deren Parent-Truth oder Claim sich semantisch geaendert hat
- neue User-Spezifizierungen

Unveraenderte Segmente behalten ihre Embeddings.

## 8. Konkrete Empfehlung fuer die lokale Technik

### Lokale SSOT

- SQLite bleibt die kanonische lokale SSOT

### Lokale Retrieval-Bausteine

- SQLite FTS5 fuer lexikale Suche
- Segment-, Claim- und Truth-Tabellen in derselben SQLite-DB
- Embeddings lokal speichern, bevorzugt in einer lokalen Vektor-Nebentabelle

Wenn der Stack schlank bleiben soll:

- zuerst nur SQLite + FTS5
- Vektor-Suche spaeter ueber `sqlite-vec` oder eine sehr kleine lokale Sidecar-Loesung

Wichtig:

- auch Embeddings und Indizes bleiben lokal
- es wird nichts nach aussen geschrieben

## 9. LLM-Einsatz: was die Modelle wirklich tun sollen

LLMs sind im Auditor nicht die Datenbank und nicht die SSOT. Sie werden gezielt fuer begrenzte Aufgaben eingesetzt:

- Claim-Extraktion aus Segmenten
- semantische Gleichsetzung oder Trennung aehnlicher Aussagen
- Reranking von Kandidaten
- Formulierung von Empfehlungen
- Synthese kleiner Entscheidungs-Pakete

Nicht ueber das Modell laufen sollte:

- komplette Repo-Reanalyse als Monoprompt
- Entscheidung, ob etwas kanonisch wahr ist, ohne Evidenz
- persistente Wahrheitsspeicherung

## 10. LiteLLM-/LLM-Uebernahme aus FIN-AI

Der Auditor soll die LLM-Architektur aus FIN-AI **nicht per Runtime-Import** nutzen. Das wuerde die entkoppelte Repo-Strategie unterlaufen.

Stattdessen ist sinnvoll:

- einen **kleinen, stabilen Teil** der FIN-AI-LiteLLM-Schicht in den Auditor zu **portieren**
- dieselben Slot- und Modellkonzepte beizubehalten
- aber FIN-AI-spezifische Seiteneffekte zu entfernen

### Zu portierende Kernbausteine aus FIN-AI

- [slot_resolver.py](/Users/martinwaelter/GitHub/FIN-AI/src/finai/core/llm/slot_resolver.py)
- [litellm_client.py](/Users/martinwaelter/GitHub/FIN-AI/src/finai/core/llm/providers/litellm_client.py)
- [embeddings.py](/Users/martinwaelter/GitHub/FIN-AI/src/finai/core/llm/embeddings.py)
- [types.py](/Users/martinwaelter/GitHub/FIN-AI/src/finai/core/llm/types.py)
- [interfaces.py](/Users/martinwaelter/GitHub/FIN-AI/src/finai/core/llm/interfaces.py)

### Was dabei bewusst entfernt oder ersetzt werden muss

- FIN-AI Request Context
- FIN-AI Graph-/Chat-Usage-Persistenz
- FIN-AI-spezifische SSE- oder UI-Events
- FIN-AI Runtime-Imports ausserhalb der LLM-Schicht

### Ziel im Auditor

Im Auditor sollte daraus eine eigene, schlanke Schicht entstehen:

- `fin_ai_auditor/core/llm/slot_resolver.py`
- `fin_ai_auditor/core/llm/providers/litellm_client.py`
- `fin_ai_auditor/core/llm/embeddings.py`
- `fin_ai_auditor/core/llm/types.py`
- `fin_ai_auditor/core/llm/interfaces.py`

### Betriebsregel

- Remote-Modelle nur ueber LiteLLM
- keine direkten Provider-SDKs in Fachpfaden
- Embeddings ebenfalls ueber die portierte LiteLLM-Schicht
- lokale Persistenz der Resultate nur im Auditor

## 11. Empfohlener Ausbaupfad

1. Segment- und Claim-Index lokal aufbauen
2. Truth-Ledger und Decision-Ledger einfuehren
3. Entscheidungs-Pakete als neue UI-SSOT definieren
4. FIN-AI LiteLLM-Kernbausteine portieren
5. zuerst FTS5 + strukturelles Retrieval, danach Hybrid Retrieval mit Embeddings
6. erst danach Paket-Synthese ueber LLMs produktiv scharf schalten

## 12. Kritische Guardrails

- Keine Vollkontext-Prompts ueber ganze Spaces oder das ganze Repo
- Keine neue User-Wahrheit ohne Scope und kanonischen Key speichern
- Keine automatische Aufloesung eines Widerspruchs ohne evidenzbasierten Claim-Abgleich
- Keine externen Schreibzugriffe vor User-Freigabe
- Keine direkte FIN-AI-Runtime-Kopplung fuer die LLM-Schicht
