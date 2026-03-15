# Forensischer Reifeplan

Dieses Dokument beschreibt den Abschlussplan fuer die forensische Reifung des
FIN-AI Auditors.

Die Zielmarke ist nicht absolute Perfektion, sondern:

- fuer definierte Kernklassen hoch belastbare Analyse
- nachvollziehbare Evidenzketten
- messbare Precision und Recall
- reproduzierbare Ergebnisse ueber reale FIN-AI-/Confluence-/Metamodell-Faelle

## Ausgangspunkt

Der Auditor ist heute stark als:

- Governance- und Soll/Ist-Auditor
- Quellenkonsolidierer
- Truth-/Decision-System
- Approval- und Writeback-kontrolliertes Arbeitswerkzeug

Er ist noch nicht stark genug als:

- nahezu beweisfaehiges forensisches Untersuchungsinstrument fuer alle
  kritischen Drift-, Ketten- und Propagationstypen.

Die wichtigste Ursache ist nicht fehlende Produktidee, sondern unvollstaendige
forensische Beweisqualitaet in Claim-, Semantik- und Finding-Schicht.

## Reifephasen

### Phase A: Klassen und Erfolgskriterien fixieren

Ziel:
- Der weitere Ausbau arbeitet gegen einen verbindlichen forensischen Katalog.

Lieferobjekte:
- [forensic-finding-classes.md](/Users/martinwaelter/GitHub/FIN-AI-Auditor/docs/forensic-finding-classes.md)
- testbarer Referenzkatalog mit stabilen Klassen-IDs

Exit:
- keine unklare Rede mehr von "forensisch gut"
- jede Kernklasse hat Zielbild, Prioritaet und Ausbauschwerpunkt

### Phase B: Referenzkorpus und Gold-Sets

Ziel:
- jede Kernklasse besitzt echte Positiv- und Negativfaelle

Lieferobjekte:
- Referenzfall-Katalog
- Positiv- und Negativfaelle pro Klasse
- Basistests, die mindestens die Katalogkonsistenz absichern

Exit:
- bekannte Fehlmuster sind nicht nur Gespraech, sondern testbar verankert

#### Aktueller Stand des Referenzkorpus

Stand 15. Maerz 2026:

- `covered`: `F01`, `F02`, `F03`, `F04`, `F05`, `F06`, `F07`, `F08`, `F09`, `F10`, `F11`, `F12`
- `partial`: derzeit keine Klasse
- `open`: derzeit keine Klasse

Wichtig:
- `covered` bedeutet hier nicht "forensisch abgeschlossen", sondern "als
  ausfuehrbarer Positiv-/Negativ-Referenzfall im Testkorpus vorhanden".
- `partial` bedeutet, dass nur ein Teil der Zielklasse belastbar im Korpus und
  in der App modelliert ist.
- `open` bedeutet, dass die Klasse bewusst als Luecke sichtbar gemacht ist und
  aktuell noch keinen ehrlichen, ausfuehrbaren Detectorpfad besitzt.

Aktueller Teilstand fuer `F12`:
- historisch/sekundaer markierte Nebenpfade
- explizit markierte `primary`-/`fallback`-/`compat`-Pfade im Code
- indirekte Pfadrollen ueber statische Call-Graph-Pfade
- mehrere Primärpfade und mehrere Nebenpfade koennen ueber qualifizierte
  Delegationsketten gegeneinander zugeordnet werden
- Varianten gleicher Rolle bleiben als getrennte Familien-Match-Gruppen sichtbar
- ein Teil der Faelle ohne explizite Rollenmarker kann jetzt ueber Drift,
  Pfadstaerke und Familienstruktur inferiert werden
- adapter-/driver-/injection-basierte Familienfaelle ohne statischen Call-Graph
  sind jetzt ebenfalls als Referenzfaelle und Detectorpfad abgedeckt

Aktueller Teilstand fuer `F09` bis `F11`:
- Risiko-Findings bleiben stabil verfuegbar
- darunter existieren jetzt explizite Support-Claims fuer
  `persist_before_enqueue`, `supersede_before_rebuild`,
  `code_missing_required_field` und `code_propagation_context`
- die Rohclaims tragen jetzt zusaetzlich explizite Pfad- und Bruchmetadaten:
  `sequence_path`, `expected_sequence_path`, `sequence_break_mode`,
  `propagation_path`, `expected_propagation_fields`
- `F09`- und `F10`-Findings liefern jetzt strukturierte Sequenzsegmente statt
  nur Wertelisten:
  `observed_sequence_path`, `expected_sequence_path`,
  `missing_sequence_segments`, `sequence_break_before`,
  `sequence_break_after`, `sequence_rejoin_at`,
  `matched_sequence_variants`
- `F11`-Findings liefern jetzt strukturierte Propagationssegmente statt nur
  fehlender Feldnamen:
  `propagation_path`, `missing_field_segments`,
  `propagation_break_before`, `propagation_break_after`,
  `propagation_rejoin_at`, `matched_propagation_variants`
- gruppierte Reaggregations- und Boundary-Faelle tragen dieselbe Struktur jetzt
  ueber mehrere Variantenpfade hinweg
- kommentierte Pseudocalls und kommentierte Manual-/Statement-Hinweise werden
  in der Python-Extraktion fuer diese Klassen jetzt nicht mehr als
  Beweisgrundlage gewertet; damit sinkt das FP-Risiko fuer `F09` und `F11`

Aktueller Teilstand fuer `F08`:
- explizite Hop-Claims fuer `Statement -[:DERIVED_FROM]-> ...` und
  `Statement -[:SUPPORTS]-> ...` werden aus Code und YAML extrahiert
- diese Hop-Claims werden im semantischen Graphen als Beziehungen zwischen den
  beteiligten Evidenzobjekten materialisiert
- `EvidenceChain.direction`- und `EvidenceChain.step.*`-Claims werden jetzt ueber
  einen gemeinsamen Semantik-Scope aggregiert, sodass Kettenpfade auch an
  zugehoerigen Findings ankommen
- ein erster belastbarer Bruchtyp ist jetzt umgesetzt:
  Statement-`DERIVED_FROM` ohne Statement-`SUPPORTS`-Hop zu `BSM_Element`
- die spiegelbildliche Luecke ist jetzt ebenfalls abgedeckt:
  Statement-`SUPPORTS` ohne vorherigen Statement-`DERIVED_FROM`-Hop
- dieser Bruchtyp liefert jetzt bereits eine erste forensische Struktur mit
  beobachtetem Pfad, erwartetem Pfad, beobachteten/erwarteten Pfadvarianten und
  expliziter Bruchstelle
- parallele aktive summary- und unit-zentrierte Kettenvarianten werden jetzt
  als eigener Implementierungskonflikt erkannt
- dokumentierter Aktivpfad und implementierter Aktivpfad koennen jetzt ebenfalls
  auf derselben Granularitaet kollidieren, statt nur ueber Richtungsheuristiken
- dokumentierte und implementierte Vollketten koennen jetzt zusaetzlich auf
  `EvidenceChain.full_path` kollidieren; fuer Code/YAML bleibt diese Vollkette
  bewusst konservativ und wird nur aus vorhandenem Summary-Hopf plus
  inferiertem `bsmAnswer`-Prefix aufgebaut
- Vollketten werden jetzt auch im semantischen Kontext von Claims und Findings
  aggregiert (`semantic_evidence_chain_full_paths`), sodass spaetere
  Mehr-Hop-Detektoren nicht erneut direkt auf Rohclaims zugreifen muessen
- `F08`-Bruchfindings tragen jetzt zusaetzlich eine erste explizite
  Bruchpositionsstruktur fuer Vollketten:
  `chain_break_index`, `chain_break_before`, `chain_break_after`,
  `observed_full_chain_path`, `expected_full_chain_path`
- dieselben Bruchfindings fuehren jetzt auch explizite Segment- und
  Variantenmetadaten:
  `chain_break_mode`, `chain_rejoin_at`, `missing_chain_segments`,
  `missing_chain_segment_path`, `remaining_expected_path`,
  `matched_break_variants`,
  `unmatched_observed_full_chain_variants`,
  `unmatched_expected_full_chain_variants`
- damit kann `F08` jetzt nicht nur einen einzelnen Punktbruch benennen, sondern
  auch mehrere gleichzeitig unvollstaendige Vollkettenvarianten in einem Finding
  zusammenfassen
- Vollketten-Widersprueche zwischen dokumentiertem und implementiertem Pfad
  werden jetzt ebenfalls mit einer ersten Divergenzstruktur angereichert:
  `full_path_divergence_index`, `full_path_divergence_documented`,
  `full_path_divergence_implemented`, `full_path_divergence_prefix`
- diese Vollketten-Divergenzen unterscheiden jetzt zusaetzlich den
  Divergenztyp und die nur auf einer Seite vorhandenen Segmente:
  `full_path_divergence_mode`,
  `full_path_documented_gap_segments`,
  `full_path_implemented_gap_segments`,
  `full_path_rejoin_at`
- dieselben Vollketten-Widersprueche tragen jetzt auch Varianten- und
  Familienmetadaten (`documented_full_chain_variants`,
  `implemented_full_chain_variants`, `documented_variant_family`,
  `implemented_variant_family`) sowie einen gemeinsamen Suffix-Kontext
  (`full_path_common_suffix`); die Paarbildung ist damit nicht mehr nur
  prefix-getrieben, sondern familienbewusster
- Vollketten-Konflikte koennen jetzt zusaetzlich mehrere Varianten als erste
  Matching-Menge fuehren:
  `matched_full_chain_pairs`,
  `unmatched_documented_full_chain_variants`,
  `unmatched_implemented_full_chain_variants`
- noch offen ist die allgemeine Mehr-Hop- und Bruchlogik, also der Uebergang
  von einzelnen Bruchtypen zu einem belastbaren Kettenbruch-Nachweis ueber
  mehrere Hops und Pfadvarianten hinweg

Noch offen fuer `F12`:
- weitere Verallgemeinerung fuer sehr tiefe oder ungewoehnliche Delegationsketten
- Robustheitsarbeit fuer schwache oder verrauschte technische Signale
- Legacy-/Nebenpfad-Befunde tragen jetzt bereits explizite Vergleichspaare
  zwischen Hauptpfad-Baseline und erkannten Nebenpfaden:
  `variant_comparisons`, `primary_source_ids`, `variant_source_ids`
- mehrere erkannte Nebenpfade koennen damit jetzt in einem Finding als erste
  Variantenmenge beschrieben werden, statt nur implizit in Rollen- und
  Delegationslisten zu erscheinen
- zusaetzlich werden jetzt erste Pfad-/Servicefamilien aus den Delegationsketten
  aggregiert:
  `primary_family_keys`, `variant_family_keys`, `variant_family_groups`
  Dadurch ist `F12` nicht mehr nur pfad-, sondern auch komponentenbezogen
  auswertbar
- dieselben Familiengruppen werden jetzt erstmals in
  `matched_variant_family_groups` und `unmatched_variant_family_groups`
  getrennt. Damit wird sichtbar, welche Nebenpfade noch innerhalb derselben
  Servicefamilie laufen und welche bereits als isolierte Parallelfamilie
  danebenstehen
- zusaetzlich stehen diese Familien jetzt auch als qualifizierte Symbolketten
  zur Verfuegung:
  `qualified_primary_family_keys`, `qualified_variant_family_keys`
  Damit wird `F12` weniger an gleichlautenden Klassennamen ausgerichtet und
  robuster gegen Namenskollisionen ueber Module hinweg
- Familienabgleich in `F12` unterscheidet jetzt explizit zwischen
  `shared_qualified_service_family`,
  `shared_name_only_service_family` und
  `isolated_service_family`
- mehrere Primärpfade werden jetzt nicht mehr implizit auf einen einzigen
  Baseline-Pfad zusammengeklappt. Nebenpfade werden über qualifizierte
  Delegationsketten dem jeweils naechstliegenden Primärpfad zugeordnet
  (`primary_group_key`, `matched_primary_variant_groups`,
  `unmatched_primary_family_groups`)
- dieselbe Zuordnung traegt jetzt erstmals einfache Kettenaehnlichkeit:
  `qualified_chain_similarity`, `chain_alignment_prefix`,
  `chain_alignment_suffix`
- der Referenzkorpus deckt fuer `F12` jetzt auch Mehrservice-Faelle mit
  mehreren Primärpfaden sowie Namenskollisionen ueber Module hinweg ab
- Varianten gleicher Rolle bleiben jetzt zusaetzlich als eigene
  Familien-Match-Gruppen sichtbar, wenn sie ueber unterschiedliche qualifizierte
  Delegationsketten laufen. `F12` verliert damit weniger Struktur in Faellen,
  in denen mehrere `fallback`- oder `compat`-Pfade parallel existieren
- die Finding-Metadaten tragen dafuer jetzt:
  `variant_family_match_groups`,
  `shared_variant_family_match_groups`,
  `isolated_variant_family_match_groups`,
  `primary_variant_family_matches`
- `F12` kann jetzt zusaetzlich einen Teil der bisher offenen Faelle ohne
  explizite Rollenmarker inferieren. Wenn mehrere unmarkierte Implementierungspfade
  fuer denselben Scope existieren, nutzt der Detector nun qualifizierte
  Delegationsketten, relative Pfadstaerke und fachlichen Drift, um einen
  schwaecheren Nebenpfad gegen einen staerkeren Primärpfad abzugrenzen
- dafuer kommen neue Finding-Metadaten hinzu:
  `inferred_path_role_inference`,
  `inferred_primary_group_keys`,
  `inferred_variant_group_keys`

### Phase C: Forensische Claim-Haertung

Ziel:
- die Claim-Schicht liefert fuer kritische Klassen nicht nur breite, sondern
  beweisfaehige Rohsignale

Hauptarbeit:
- explizite Ketten-Claims
- temporale Sequenz-Claims
- Feld-Propagation-Claims
- Pfadfamilien-Claims fuer Haupt- vs Nebenpfad
- sauberere Negativbeweise fuer dokumentierte, aber nicht nachweisbare Pfade

Betroffene Dateien:
- [claim_extractor.py](/Users/martinwaelter/GitHub/FIN-AI-Auditor/src/fin_ai_auditor/services/claim_extractor.py)
- [bsm_domain_claim_extractor.py](/Users/martinwaelter/GitHub/FIN-AI-Auditor/src/fin_ai_auditor/services/bsm_domain_claim_extractor.py)

Exit:
- fuer `F03`, `F04`, `F08`, `F09`, `F10`, `F11`, `F12` existieren belastbare
  Rohclaims

### Phase D: Forensische Semantik und Finding-Engine

Ziel:
- aus forensischen Rohclaims entstehen explizite, priorisierte Finding-Klassen

Hauptarbeit:
- neue bzw. geschaerfte Kategorien:
  - chain_break
  - eventual_consistency_gap
  - propagation_gap
  - legacy_path_gap
  - undocumented_implementation_path
- Evidenz- und Schweregradlogik an Beweisstaerke koppeln

Betroffene Dateien:
- [semantic_graph_service.py](/Users/martinwaelter/GitHub/FIN-AI-Auditor/src/fin_ai_auditor/services/semantic_graph_service.py)
- [finding_engine.py](/Users/martinwaelter/GitHub/FIN-AI-Auditor/src/fin_ai_auditor/services/finding_engine.py)
- [bsm_domain_contradiction_detector.py](/Users/martinwaelter/GitHub/FIN-AI-Auditor/src/fin_ai_auditor/services/bsm_domain_contradiction_detector.py)

Exit:
- Kernklassen sind im System nicht nur implizit, sondern explizit modelliert

### Phase E: Messung und FN/FP-Abbau

Ziel:
- forensische Qualitaet wird messbar und gezielt verbessert

Hauptarbeit:
- Precision/Recall pro Klasse
- FN-/FP-Listen pro Klasse
- Regression gegen reale Fehlmuster statt generischer Mustererweiterung

Exit:
- jede Klasse besitzt einen nachvollziehbaren Qualitaetsstatus
- kritische Klassen koennen mit harten Gates bewertet werden

### Phase F: Forensischer Pilot

Ziel:
- reale Eignung im echten Arbeitskontext nachweisen

Hauptarbeit:
- 20 bis 30 reale Faelle
- Treffer, Fehlalarme, uebersehene Befunde und Evidenzqualitaet dokumentieren
- Go-/No-Go fuer forensische Nutzung treffen

Exit:
- kein theoretischer Reifegrad mehr, sondern belegter Pilotstand

## Priorisierungslogik

Die Ausbaureihenfolge folgt nicht technischer Schoenheit, sondern fachlichem
Schadenspotenzial:

1. `F03`, `F04`, `F06`, `F08`, `F09`, `F10`, `F11`
2. `F01`, `F02`, `F07`
3. `F05`, `F12`

Begruendung:
- Policy-, Ketten-, Temporal- und Propagation-Luecken sind fuer eure
  forensische Zielklasse schaedlicher als rein kosmetische Dokuabweichungen.

## Nicht-Ziele waehrend der forensischen Reifung

Bis die Kernklassen belastbar stehen, sollen diese Dinge keine Prioritaet
bekommen:

- neue groessere UI- oder Visualisierungszweige
- neue Komfortmodule ohne Einfluss auf Beweisqualitaet
- weitere Integrations- oder Writeback-Ziele
- breit angelegte neue Detectorfamilien ohne Referenzfallbasis

## Freigabedefinition fuer forensische Nutzung

Eine ehrliche Freigabe als forensisch belastbares System setzt mindestens voraus:

- Klassen `F01` bis `F04` und `F06` bis `F11` sind mit Referenzfaellen abgedeckt
- kritische False Positives und False Negatives sind pro Klasse bekannt und
  ruecklaeufig
- Findings besitzen nachvollziehbare Evidenzketten statt nur plausibler Texte
- der Pilotbetrieb bestaetigt die Nutzbarkeit im realen Kontext

## Naechste konkrete Arbeitsschritte

1. Referenzkatalog im Testbereich fixieren
2. Positiv- und Negativfaelle pro Klasse systematisch sammeln
3. fuer `F08` bis `F11` zuerst die Claim-Luecken schliessen
4. danach die Finding-Engine fuer diese Klassen explizit machen
