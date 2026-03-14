# Delta, Synchronisierung und Widerspruchsaufloesung

## Ziel

Der Auditor muss mit grossen und sich staendig aendernden Datenmengen umgehen, ohne bei jedem Lauf das komplette Universum neu im Kontext zu halten. Deshalb braucht er einen inkrementellen, evidenzbasierten Delta-Mechanismus.

Das Kernprinzip lautet:

- Quellen werden versioniert beobachtet
- Findings werden an stabile Evidenzanker gebunden
- Aufloesungen werden als eigene Fakten gespeichert
- Re-Audits laufen inkrementell und heben nur betroffene Cluster neu an
- externe Quellen bleiben bis zur User-Freigabe read-only; schreibbar ist nur die lokale Auditor-DB

## Grundarchitektur

### 1. Source Registry

Jede beobachtete Quelle bekommt einen stabilen Schluessel:

- GitHub: `repo + ref + path`
- Confluence: `cloud/site + page_id`
- Metamodell: `operation_id + parameter_hash`
- lokale Doku: absoluter Pfad oder Repo-relativer Pfad

Jira gehoert bewusst nicht zur Analyse-Source-Registry. Fuer das lokale Vollzugsledger
spaeterer Codeaenderungs-Tickets reichen dort stabile Schluessel wie `site + ticket_key`.

Pro Quelle braucht der Auditor mindestens:

- letzte bekannte Revision
- letzter `content_hash`
- letzter erfolgreicher Sync
- letzter Lauf mit Findings

### 2. Snapshot Chain

Jeder Scan erzeugt einen `source_snapshot`. Ein Snapshot zeigt nicht nur den aktuellen Stand, sondern verweist per `parent_snapshot_id` auf den letzten bekannten Stand derselben Quelle.

Damit lassen sich drei Fragen robust beantworten:

- Hat sich die Quelle ueberhaupt veraendert?
- Ist nur Metadaten-Rauschen passiert oder inhaltliche Aenderung?
- Welche Findings muessen neu bewertet werden?

### 3. Stable Anchors

Findings duerfen nicht nur auf freie Textfragmente zeigen. Jede Evidenz braucht einen stabilen Anchor.

Empfohlene Prioritaet:

1. strukturierter Anchor
   - Datei + Symbol + Line-Range
   - Confluence Heading-Path + Block-Index
2. semistabiler Anchor
   - Section Path + `snippet_hash`
3. Fallback
   - `content_hash` + Volltextfenster

Ein Finding bleibt auch dann wiederfindbar, wenn sich Zeilennummern verschieben, solange Heading-Pfad oder Snippet-Fingerprint stabil genug bleiben.

## Delta-Mechanismus

### Stufe A: Source Delta

Beim Import wird zunaechst nur geprueft:

- neue Revision?
- neuer `content_hash`?
- neue Strukturanker?

Wenn nein:

- Quelle nicht erneut claim-normalisieren
- bestehende Findings bleiben unveraendert

Wenn ja:

- nur diese Quelle und ihre betroffenen Cluster neu analysieren

### Stufe B: Anchor Delta

Innerhalb einer geaenderten Quelle werden Anchors diff-basiert gemappt:

- exakter Anchor-Match
- Heading-/Path-Match
- Snippet-Fingerprint-Match
- semantischer Fallback-Match

Ergebnis:

- `unchanged`
- `moved`
- `rewritten`
- `deleted`
- `split`
- `merged`

Nur `rewritten`, `deleted`, `split` und `merged` erzwingen eine fachliche Neubewertung.

### Stufe C: Claim Delta

Claims werden pro Anchor normalisiert und erhalten einen deterministischen Fingerprint:

- `subject_kind`
- `subject_key`
- `predicate`
- `normalized_value`
- `scope`

Wenn sich nur Formulierungen aendern, aber der Fingerprint gleich bleibt, entsteht kein neues fachliches Delta.

Wenn sich der Claim-Fingerprint aendert:

- alte Findings werden auf Relevanz geprueft
- neue Findings koennen erzeugt werden
- bestehende Findings koennen auf `superseded` gehen

## Umgang mit widerspruechlichen Aufloesungen

Widersprueche loesen sich nicht dadurch, dass ein neuer Lauf den alten Text nicht mehr sieht. Der Auditor muss Aufloesungen explizit speichern.

Deshalb braucht die naechste Ausbaustufe zusaetzlich:

- `decision` oder `resolution` Tabelle
- Verknuepfung zu `finding_id` und spaeter `claim_id`
- Geltungsbereich: global, pro Objekt, pro Seite, pro Prozess
- Status: vorgeschlagen, bestaetigt, verworfen, ersetzt

### Regel fuer smarte Aufloesungserkennung

Ein Widerspruch gilt nur dann als aufgeloest, wenn mindestens eine der folgenden Bedingungen zutrifft:

- beide Seiten des Widerspruchs konvergieren auf denselben Claim-Fingerprint
- eine bestaetigte Entscheidung markiert eine Seite explizit als kanonisch
- der alte Anchor ist geloescht und durch einen neuen Anchor mit kompatibler Aussage ersetzt

Nicht ausreichend ist:

- Text wurde verschoben
- eine Quelle ist temporaer nicht erreichbar
- ein LLM behauptet semantische Aehnlichkeit ohne stabile Evidenz

## Wie grosse Datenmengen sicher beherrschbar bleiben

### Harte Trennung zwischen Volltext und Audit-Kern

Die SQLite-DB soll nicht die komplette Confluence und nicht den kompletten Code als SSOT speichern. Sie speichert nur:

- Snapshot-Metadaten
- stabile Anchors
- Hashes
- normalisierte Claims
- Findings
- Beziehungen
- Entscheidungen

Rohinhalte werden nur bei Bedarf geladen oder ausserhalb der Kern-SSOT in einem Cache abgelegt.
Auch dieser Cache bleibt lokal; externe Systeme werden dabei nicht veraendert.

### Cluster statt Vollkontext

Die UI und spaetere LLM-Schritte arbeiten nicht auf allen Quellen zugleich, sondern auf kleinen Konfliktclustern:

- Objekt
- Prozessschritt
- Seite + Service
- Claim-Gruppe

Damit bleibt der Arbeitskontext begrenzt und nachvollziehbar.

### Re-Audit nur fuer betroffene Cluster

Wenn eine Confluence-Seite geaendert wurde, muessen nicht alle Findings neu bewertet werden. Es werden nur die Cluster neu gerechnet, die ueber `source_snapshot`, `anchor` oder `claim` davon abhaengen.

## Empfohlene technische Umsetzung

### Jetzt sinnvoll

- SQLite als lokale Delta-SSOT
- `source_snapshots` mit `parent_snapshot_id`, `revision_id`, `content_hash`
- `finding_locations` mit strukturierten Anchors und Hashes
- `finding_links` fuer Widerspruchs- und Lueckenbeziehungen

### Als naechstes sinnvoll

- `claims` Tabelle
- `claim_links` oder Claim-zu-Finding-Mapping
- `decisions` Tabelle
- `source_heads` oder `source_registry` fuer den letzten bekannten Quellstand
- inkrementeller Collector pro Quelle

## Operative Regeln

- Nie ganze Confluence-Spaces blind neu ziehen und komplett neu prompten.
- Nie Findings ohne Anchor und `content_hash` persistieren.
- Nie einen Widerspruch als geloest markieren, nur weil eine Quelle verschwunden ist.
- Jede automatische Aufloesung braucht reproduzierbare Evidenz und muss im Zweifel review-pflichtig bleiben.
- Bis zu einer expliziten User-Entscheidung keine externen Schreibzugriffe ausfuehren.
