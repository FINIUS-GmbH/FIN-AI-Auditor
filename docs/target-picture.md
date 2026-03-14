# Zielbild

Dieses Dokument ist das kanonische Zielbild fuer den `FIN-AI Auditor`.

Es beschreibt:

- warum das Produkt existiert
- was das Produkt fachlich und technisch leisten soll
- welche Grenzen und Governance-Regeln gelten
- wie der Soll-Endzustand fuer UI, Analyse, Delta-Logik, Truth-Ledger und Writeback aussieht
- in welchen sinnvollen Ausbaustufen das Zielbild erreicht werden soll

## 1. Produktauftrag

`FIN-AI Auditor` ist ein eigenstaendiges Governance- und Audit-Produkt fuer FIN-AI.

Der Auditor soll die Luecke schliessen zwischen:

- fachlicher Beschreibung
- prozessualer Beschreibung
- Metamodell
- Guardrails
- realem Lese- und Schreibverhalten im Code

Das Produkt soll nicht nur Abweichungen finden, sondern aus ihnen einen kontrollierten Entscheidungs- und Verbesserungsprozess machen.

## 2. Vision

Die Zielvision ist ein Werkzeug, mit dem ein fachlicher oder technischer Verantwortlicher jederzeit belastbar beantworten kann:

- Was ist fuer ein Objekt, einen Prozessschritt oder einen Status fachlich eigentlich gewollt?
- Was sagt die aktuelle Dokumentation wirklich?
- Was macht der Code tatsaechlich?
- Was sagt das Metamodell dazu?
- Wo widersprechen sich diese Ebenen?
- Welche fehlenden Definitionen oder unklaren Annahmen blockieren eine saubere Umsetzung?
- Welche kleinsten Entscheidungen sind jetzt noetig, um aus unklarer Lage wieder einen konsistenten Sollzustand zu machen?

Die langfristige Zielmarke ist:

- Spezifikationsgetriebene Entwicklung wird real moeglich.
- AI-Coding bekommt belastbaren, pruefbaren Kontext statt widerspruechlicher Inputs.
- Confluence wird wieder zur belastbaren SSOT fuer fachliche und prozessuale Aussagen.
- Jira-Tickets fuer Codeaenderungen enthalten genug fachliche Tiefe, dass ein AI-Coding-Agent sie vollstaendig und pruefbar umsetzen kann.

## 3. Erfolgsdefinition

Das Produkt ist erfolgreich, wenn folgende Ergebnisse stabil erreicht werden:

- Widersprueche zwischen Code, Confluence und Metamodell werden frueh und reproduzierbar sichtbar.
- Textuelle Umformulierungen werden nicht als fachliche Konflikte fehlklassifiziert.
- User muessen nicht mit rohen Findings arbeiten, sondern mit kleinen, fachlich sinnvollen Entscheidungspaketen.
- User-Spezifizierungen gehen nicht verloren, sondern werden als Wahrheiten gespeichert und beeinflussen kuenftige Analysen.
- Delta-Re-Audits bewerten nicht jedes Mal alles neu, sondern nur betroffene Cluster.
- Spaetere externe Aenderungen auf Confluence und Jira passieren nur kontrolliert, nachvollziehbar und nach expliziter Freigabe.

## 4. Produktgrenzen

`FIN-AI Auditor` ist bewusst nicht Teil der FIN-AI Runtime.

Das bedeutet:

- eigenes Repo
- eigener Build
- eigener Deployment-Stack
- keine Runtime-Imports aus FIN-AI
- keine Mitverdrahtung in FIN-AI Deployments
- keine verdeckte Koppelung an interne FIN-AI API-Pfade, wenn stabile Direktquellen moeglich sind

Der Auditor analysiert FIN-AI, ist aber nicht FIN-AI.

## 5. Betriebs- und Governance-Regeln

Bis zu einer expliziten User-Entscheidung gelten diese Regeln:

- externe Systeme werden nur lesend angesprochen
- die einzige schreibende SSOT ist die lokale Auditor-Datenbank
- Jira ist kein Analyseinput, sondern nur Ziel fuer spaetere Codeaenderungs-Tickets
- Confluence ist Analysequelle und spaeter optionales Doku-Ziel
- das Metamodell wird direkt read-only gelesen und lokal als aktueller Dump vorgehalten

Wichtige Schutzregel:

- kein externer Writeback ohne expliziten Freigabeschritt

## 6. Quelllandschaft und Rollen der Quellen

### 6.1 Lokales FIN-AI Repo

Primaere Codequelle ist der lokale FIN-AI Checkout im GitHub-Verzeichnis.

Zweck:

- Code-Realitaet lesen
- Read-/Write-Pfade erkennen
- Lifecycle-, Policy- und Scope-Logik extrahieren
- lokale `_docs` als repo-nahe Evidenz einbeziehen

### 6.2 Confluence

Confluence ist die primaere externe Dokuquelle.

Zweck:

- fachliche Aussagen lesen
- Prozessbeschreibungen lesen
- Definitionen, Begriffe und Entscheidungen lesen
- spaeter punktuelle Patch-Previews und kontrollierte Updates vorbereiten

### 6.3 Metamodell

Das Metamodell ist die technische und prozessuale Strukturquelle.

Zweck:

- Phasen, Fragen, Objekte und Beziehungen als Referenz lesen
- den aktuellen Dump pro Lauf aktualisieren
- Metamodell-Drift gegen Doku und Code sichtbar machen

### 6.4 Jira

Jira wird nicht lesend in die Analyse eingebunden.

Zweck:

- spaeter strukturierte Tickets fuer FIN-AI Codeaenderungen erzeugen
- nur nach expliziter User-Freigabe extern schreiben

## 7. Kernfaehigkeiten im Zielzustand

Der Zielzustand des Produkts umfasst sieben zusammenhaengende Faehigkeitsbereiche.

### 7.1 Read-only Analyse

Der Auditor liest:

- lokales FIN-AI Repo
- lokale Doku
- Confluence
- Metamodell

und erzeugt daraus:

- Snapshots
- Segmente
- Claims
- Truth-Kontext
- Findings
- Entscheidungspakete

### 7.2 Atomare Problemelemente und Entscheidungspakete

Nach jedem Lauf sieht der User:

- Bewertungskategorien
- darin kleine, konkrete Problemelemente
- gruppiert in wenige, fachlich verdauliche Entscheidungspakete

Jedes Problemelement erklaert:

- was das Problem ist
- welche Quellen betroffen sind
- welche Aussagen dort stehen
- welche Empfehlung der Auditor ableitet

### 7.3 Truth-Ledger

User-Kommentare und Spezifizierungen werden nicht als Freitext-Notizen behandelt.

Sie werden als lokale Wahrheiten gespeichert und haben fachliche Wirkung auf:

- Claim-Bewertung
- Widerspruchslogik
- Paket-Regenerierung
- spaetere Ticket- und Patch-Vorschlaege

### 7.4 Semantische Widerspruchslogik

Der Auditor soll nicht nur Wortlaute vergleichen, sondern fachliche Bedeutungen.

Das Ziel ist:

- textuelle Varianten erkennen
- echte semantische Konflikte von rein sprachlicher Variation trennen
- Policy-, Lifecycle-, Scope- und Read/Write-Konflikte gezielt erkennen

### 7.5 Inkrementelle Delta-Neubewertung

Der Auditor soll bei Aenderungen nicht jedes Mal das ganze Universum neu bewerten.

Das Ziel ist:

- geaenderte Quellen erkennen
- stabile Anchors wiederfinden
- Claim-Deltas bestimmen
- nur betroffene Cluster neu berechnen

### 7.6 Kontrollierter Writeback

Spaeter soll der Auditor:

- Confluence-Aenderungen als Patch-Preview vorbereiten
- Jira-Tickets fuer Codeaenderungen erstellen

Aber:

- erst Draft und Review
- dann Freigabe
- dann kontrollierte Ausfuehrung
- danach Verifikation und lokales Vollzugsledger

### 7.7 AI-gestuetzte, aber evidenzbasierte Unterstuetzung

LLMs sind im Auditor Hilfsmittel, nicht SSOT.

Ziel ist:

- erst Evidenz
- dann Normalisierung
- dann semantische Verdichtung
- dann Vorschlag

Nicht Ziel ist:

- freie Volltextumschreibung ohne Anchor
- unbelegte Halluzinations-Empfehlung
- ungepruefte Massenkorrektur

## 8. Zielbild der UI

Die UI ist als Arbeitsoberflaeche gedacht, nicht als Marketing-Ansicht.

### 8.1 Grundlayout

Zielbild:

- kompakter Steuerbereich fuer Runs und Quellen
- grosser Arbeitsbereich fuer Findings, Pakete und Evidenz
- sichtbarer Statusbereich mit Fortschritt und KI-Log
- sichtbarer Bereich fuer umgesetzte Aenderungen

### 8.2 Wichtige UI-Bereiche

Der Zielzustand der UI umfasst:

- Run-Anlage
- Fortschrittsbalken mit aktuellen Phasen
- Entscheidungspakete nach Kategorien
- Evidenz- und Quellenansicht
- Truth-Ledger
- Approval-Queue
- KI-Statuslog
- Vollzugsledger fuer umgesetzte Aenderungen

### 8.3 Status- und KI-Log

Der User soll nachvollziehen koennen:

- was die aktuelle Analysephase macht
- welche Kommentare des Users wie interpretiert wurden
- welche Wahrheiten daraus entstanden
- welche Pakete und Cluster dadurch neu gewichtet werden

### 8.4 Ledger fuer umgesetzte Aenderungen

Im Zielzustand listet die UI klar und knapp:

- welche Confluence-Seite aktualisiert wurde
- welches Jira-Ticket erstellt wurde
- welche Freigabe dazu fuehrte
- welche betroffenen Findings oder Pakete dadurch referenziert sind

## 9. Zielbild der Analysepipeline

### 9.1 Collector Layer

Der Collector Layer liest aus allen aktiven Quellen read-only und ueberfuehrt sie in Snapshots und Dokumente.

### 9.2 Segment Layer

Quellen werden in stabile Segmente zerlegt:

- Code: Datei, Klasse, Funktion, Router-Handler, Zeilenbereich
- Confluence: Seite, Heading-Pfad, Blockanker
- Metamodell: Node, Relation, Property, Dumpanker
- lokale Doku: Datei, Abschnitt, Bereich

### 9.3 Claim Layer

Aus Segmenten entstehen atomare Claims.

Ein Claim beschreibt typischerweise:

- Objekt
- Property
- Predicate
- normalisierten Wert
- Scope
- Evidenzanker

### 9.4 Truth Layer

Aktive lokale Wahrheiten werden wie priorisierte Claims behandelt.

Prioritaet im Zielbild:

1. aktive User-Wahrheit
2. bestaetigte Entscheidung
3. Metamodell
4. dokumentierte Aussage
5. implementierte Aussage
6. heuristische oder LLM-gestuetzte Inferenz

### 9.5 Finding Layer

Aus Claims entstehen Problemelemente wie:

- Widersprueche
- Klarstellungen / fehlende Informationen
- Implementierungsdrift
- Read-/Write-Luecken
- Terminologiekonflikte
- Ownership-Luecken
- Traceability-Luecken
- Policy-Konflikte
- veraltete Quellen
- Low-Confidence Review

### 9.6 Package Layer

Verwandte Problemelemente werden zu kleinen Entscheidungspaketen geclustert.

Clusteranker sind typischerweise:

- gleiches Objekt
- gleiche Property
- gleicher Prozessschritt
- gleiche Doku-Seite plus gleicher Service
- gleicher Widerspruchskern

### 9.7 Recommendation Layer

Auf Basis von Evidenz, Wahrheiten, Retrieval-Kontext und Semantik entstehen Empfehlungen.

Diese muessen:

- kurz
- pruefbar
- handlungsorientiert
- begruendet

sein.

## 10. Zielbild fuer Delta und Retrieval

Der Auditor soll grosse und aenderliche Datenmengen beherrschen, ohne dauernd alles neu in den Kontext zu laden.

### 10.1 Source Registry

Pro Quelle wird der letzte bekannte Stand vorgehalten:

- Revision
- Content-Hash
- letzter erfolgreicher Sync
- letzter Snapshot

### 10.2 Snapshot Chain

Jeder Lauf erzeugt pro Quelle einen Snapshot mit Rueckverweis auf den letzten bekannten Zustand.

### 10.3 Stable Anchors

Findings und Claims werden an stabile Anker gebunden:

- Codeanker
- Section- und Heading-Anker
- Snippet-Hashes
- Strukturanker

### 10.4 Claim Delta

Das Zielbild unterscheidet mindestens:

- `exact`
- `textual_only`
- `semantic`
- `new_identity`

### 10.5 Impact Re-Evaluation

Nur betroffene Scope-Cluster und ihre Wahrheiten werden neu bewertet.

### 10.6 Hybrid Retrieval

Der Zielzustand kombiniert:

- strukturelle Suche
- lexikale Suche
- semantische Suche

und priorisiert kleine, relevante Kontextfenster fuer:

- Empfehlungsgenerierung
- Delta-Neubewertung
- spaetere Patch- und Ticketerzeugung

## 11. Zielbild fuer User-Entscheidungen

Der User kann pro Paket oder Problemelement:

- annehmen
- ablehnen
- spezifizieren

### 11.1 Annehmen

Die Empfehlung wird als bevorzugte Aufloesungsrichtung uebernommen.

### 11.2 Ablehnen

Die Empfehlung wird verworfen, das Problem bleibt aber nachvollziehbar erhalten.

### 11.3 Spezifizieren

Der User fuegt neue fachliche Wahrheit hinzu.

Diese Wahrheit:

- wird lokal persistiert
- kann bestehende Wahrheiten supersedieren
- kann mehrere Pakete gleichzeitig beeinflussen
- darf spaetere Re-Audits fachlich umlenken

## 12. Zielbild fuer Jira-Tickets zur Codeaenderung

Jira-Tickets muessen so vollstaendig sein, dass ein AI-Coding-Agent oder Entwickler damit nicht raten muss.

Ein Ticket im Zielzustand enthaelt mindestens:

- klare Problembeschreibung
- fachlichen und technischen Grund
- konkrete Korrekturmassnahmen
- erwartetes Zielbild
- pruefbare Abnahmekriterien
- Implikationen und Seiteneffekte
- betroffene Dateien, Services, Prozesse oder Objekte
- referenzierte Evidenz
- Implementierungshinweise
- Validierungsschritte
- einen expliziten AI-Coding-Prompt

## 13. Zielbild fuer Confluence-Patches

Confluence-Aenderungen sollen nicht als Vollseiten-Rewrite erfolgen.

Der Zielzustand ist:

- section-anchored Patches
- Before/After-Preview
- Review-Markierungen im Arbeitsmodus
- kontrolliertes Apply
- Verifikation nach dem Writeback

Review-Markierungen im Arbeitsmodus:

- rot + durchgestrichen fuer zu entfernende Passagen
- gelb fuer korrigierte oder neue Passagen
- gruen fuer bestaetigte Entscheidungen

Die produktive Zielseite soll nach Freigabe wieder lesbar und nicht dauerhaft als Review-Protokoll verschmutzt sein.

## 14. Technische Zielarchitektur

Der Zielzustand des Systems umfasst:

- API fuer Runs, Ledger, Approval und Execution
- Worker fuer Analyse, Delta, Retrieval, Patch-Preview und Ticketing
- React/Vite Workbench
- lokale relationale Audit-SSOT
- optionale lokale Embedding-Caches
- portierte LiteLLM-Schicht ohne FIN-AI Runtime-Kopplung

### 14.1 Lokale SSOT

Die lokale DB ist im Zielbild die kanonische Governance-SSOT fuer:

- Runs
- Snapshots
- Findings
- Finding-Relationen
- Claims
- Truths
- Entscheidungspakete
- Entscheidungen
- Approval-Requests
- umgesetzte Aenderungen
- Retrieval-Segmente und Claim-Links
- OAuth-Zustand und lokale Tokens

### 14.2 Trennung von Analyse und Ausfuehrung

Es gibt zwei klar getrennte Pfade:

- Analysepfad
- Delivery-Pfad

Analysepfad:

- liest nur
- erzeugt lokale Evidenz und Vorschlaege

Delivery-Pfad:

- startet erst nach Freigabe
- fuehrt kontrollierte externe Aktionen aus
- vermerkt den Vollzug lokal

## 15. Nicht-Ziele

Nicht Ziel des Produkts ist:

- FIN-AI Code direkt selbst zu aendern
- ohne Freigabe Confluence oder Jira extern zu schreiben
- jede Frage vollautomatisch ohne User zu entscheiden
- LLMs als Wahrheitsquelle zu behandeln
- Vollkontext aller Systeme permanent im Prompt zu halten
- FIN-AI Deployment oder Runtime direkt mitzuziehen

## 16. Hauptrisiken

Die wichtigsten Risiken im Zielbild sind:

- zu grobe Claim-Extraktion
- semantische Fehlklassifikation bei schwacher Evidenz
- zu grosse Entscheidungspakete
- schlechte Anchor-Stabilitaet bei Doku-Aenderungen
- Ticket-Flut statt klarer Cluster
- zu frueher Writeback ohne ausreichend harte Freigabeschranken

## 17. Delivery-Plan zum Zielbild

### Stufe 1: Read-only Audit MVP

Ziel:

- echte Quellen lesen
- Claims extrahieren
- Findings anzeigen
- lokalen Audit-Speicher aufbauen

### Stufe 2: Entscheidungspakete und Truth-Ledger

Ziel:

- kleine Pakete statt flacher Finding-Listen
- User-Spezifizierungen als Wahrheiten speichern
- lokale Neugewichtung vorbereiten

### Stufe 3: Semantik, Delta und Retrieval

Ziel:

- semantische Widerspruchslogik
- semantische vs. textuelle Deltas
- nur betroffene Cluster neu bewerten
- Retrieval-Kontexte robust und skalierbar machen

### Stufe 4: Confluence Patch Preview

Ziel:

- section-anchored Diffs
- Review-Markierungen
- Vorschau ohne externen Vollzug

### Stufe 5: Jira Drafting und Approval

Ziel:

- Ticket-Briefs mit vollem AI-Coding-Kontext
- Approval-Flow
- kontrollierte Execution-Vorbereitung

### Stufe 6: Kontrollierter externer Writeback

Ziel:

- Jira-Tickets wirklich erstellen
- spaeter Confluence wirklich aktualisieren
- Vollzug und Verifikation lokal dokumentieren

### Stufe 7: Betriebsreife

Ziel:

- robustes OAuth- und Token-Handling
- bessere semantische Modelle
- skalierbares Retrieval
- stabiler Team-Betrieb

## 18. Aktuelle strategische Prioritaet

Die aktuell wichtigste Linie zum Zielbild ist:

- Analysequalitaet weiter steigern
- Confluence-Live-Read sauber produktiv anbinden
- Jira-Writeback erst nach hartem Approval wirklich scharf schalten

## 19. Kanonische Lesereihenfolge

Fuer neue Beteiligte ist die empfohlene Lesereihenfolge:

1. dieses Zielbild
2. Architektur
3. Produktscope
4. Datenmodell
5. Entscheidungs-Pakete und Retrieval
6. Delta und Aufloesungsstrategie
7. Roadmap
