# Produktscope

Der kanonische Produktauftrag, die Vision und die Governance-Grenzen sind im
[Zielbild](./target-picture.md) festgezogen. Diese Datei beschreibt den
praktischen Produktscope und die bewusst gesetzten Produktgrenzen.

## Produktname

`FIN-AI Auditor`

## Primarnutzen

Das Produkt soll Spezifikations-, Dokumentations- und Implementierungsdrift sichtbar machen und in einen geregelten Verbesserungsprozess ueberfuehren.

## Kernfaehigkeiten

### Phase 1

- Audit-Run anlegen
- Quellenbereich festlegen
- lokalen FIN-AI-Checkout als primaere Codequelle verwenden
- Findings anzeigen
- Evidenzen je Finding anzeigen
- Empfehlungen fuer Doku- und Codekorrekturen ausgeben
- Problemsignale in kleine Entscheidungspakete vorbereiten

### Phase 2

- Rueckfragen-Workflow fuer unklare Claims
- Finding-Clustering nach Objekt, Phase, Prozess oder Doku-Seite
- Entscheidungspakete nach Kategorien und atomaren Problemelementen
- Truth-Ledger fuer User-Spezifizierungen
- Confluence-Patch-Preview mit Review-Markierungen, weiterhin nur lokal
- Jira-Ticket-Drafts mit Acceptance Criteria und AI-Prompt, weiterhin nur lokal

### Phase 3

- inkrementelle Audits gegen neue Commits oder geaenderte Seiten
- automatische Delta-Erkennung
- Freigabe- und Publishing-Workflow
- policy-basierte Auto-Vorschlaege

## Aktueller Fokus gegen das Zielbild

Die naechste inhaltliche Prioritaet ist:

- echte Jira-/Confluence-E2E-Verifikation gegen kontrollierte Testziele
- Last-, Recovery- und Wiederanlauf-Haertung fuer den Dauerbetrieb
- abschliessende Pilot- und Go-Live-Gates fuer produktive Nutzung

## Nicht-Ziele fuer den Start

- kein vollautomatisches Live-Ueberschreiben von Confluence-Seiten
- keine unkontrollierten Jira-Massenanlagen
- keine direkte Aenderung von FIN-AI Code
- keine verdeckte Kopplung an FIN-AI Deployments
- keine externen Schreibzugriffe vor expliziter User-Entscheidung

## Nutzerrollen

- Governance Owner
- Business Analyst
- Architekturverantwortliche
- Entwickler
- Dokumentationsverantwortliche

## Hauptrisiken

- falsche oder zu grobe Claim-Extraktion
- zu fruehe LLM-Automatisierung ohne ausreichende Evidenz
- instabile Confluence-Diffs durch Vollseiten-Updates
- Ticket-Flut statt brauchbarer Clusterung

## Produktprinzipien

- belegbar vor intelligent
- kleine, reviewbare Aenderungen statt Big Bang
- aus einer Entscheidung mehrere betroffene Stellen konsistent ableiten
- produktive Doku bleibt SSOT, Review-Markierung ist nur Arbeitsmodus
- externe Systeme bleiben bis zur Freigabe read-only; nur die lokale Auditor-DB ist schreibbar
- User-Spezifizierungen werden als Wahrheiten gespeichert, nicht als lose Kommentare
