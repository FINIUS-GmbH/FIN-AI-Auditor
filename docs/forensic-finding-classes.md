# Forensische Finding-Klassen

Dieses Dokument definiert die forensischen Kernklassen, an denen die weitere
Reifung des FIN-AI Auditors gemessen wird.

Ziel ist nicht, beliebig viele Findings zu haben, sondern die fachlich
kritischen Klassen reproduzierbar, evidenzbasiert und mit klarer
False-Positive-/False-Negative-Bewertung abzudecken.

## Bewertungslogik

Jede Klasse wird entlang derselben vier Fragen bewertet:

- `Zielzustand`: Was muss der Auditor fuer diese Klasse belastbar koennen?
- `Heutiger Stand`: Wie stark ist die aktuelle Implementierung?
- `Beweisbedarf`: Welche Evidenz muss vorliegen, damit ein Befund wirklich
  forensisch tragfaehig ist?
- `Ausbauschwerpunkt`: Wo muss die naechste Härtung ansetzen?

## Klassenmatrix

| ID | Klasse | Zielzustand | Heutiger Stand | Beweisbedarf | Ausbauschwerpunkt | Prioritaet |
| --- | --- | --- | --- | --- | --- | --- |
| `F01` | Doku-gegen-Doku-Widerspruch | Widerspruechliche Confluence- oder lokale Doku-Aussagen werden frueh und reproduzierbar erkannt. | Stark | mindestens zwei widerspruechliche Claim-Quellen mit stabilem Scope | Praezision halten, Fehlklassifikation sprachlicher Varianten weiter senken | `P0` |
| `F02` | Doku-gegen-Metamodell-Widerspruch | Fachaussagen und Modellvertrag werden auf denselben Scope normiert und belastbar verglichen. | Stark | dokumentierte Aussage plus Metamodell-Claim auf identischem Scope | Modelltiefe fuer weitere Objekte und Labels ausbauen | `P0` |
| `F03` | Doku-gegen-Code-Drift | Dokumentierte Soll-Regel und implementiertes Ist-Verhalten werden mit klarer Evidenz gegeneinander gestellt. | Mittel | dokumentierter Soll-Claim plus belastbarer Code-Claim mit identischem Objekt/Predicate | Code-Claim-Tiefe, Policy-/Lifecycle-Semantik und Scope-Normalisierung schaerfen | `P0` |
| `F04` | Dokumentierter Read-/Write-Pfad fehlt im Code | Der Auditor kann belastbar sagen, dass ein dokumentierter Pfad im Code nicht nachweisbar ist. | Mittel | dokumentierter Pfad-Claim plus ausbleibender Code-Nachweis trotz belastbarer Suchbasis | Negativbeweislogik und Codepfad-Nachweis schaerfen | `P0` |
| `F05` | Codepfad ist fachlich nicht dokumentiert | Relevantes Implementierungsverhalten ohne Doku-Abdeckung wird sichtbar, nicht nur Doku-vs-Code-Drift. | Schwach | belastbarer Codepfad plus fehlender fuehrender Doku-/Truth-Kontext | spec-driven Finding-Engine um diese Klasse ergaenzen | `P1` |
| `F06` | Policy-/Approval-/Allowlist-Verstoss | Guardrail-, Scope- und Approval-Verletzungen werden deterministisch erkannt und sauber begruendet. | Stark | Policy-Claim plus abweichender Code-/Config-/Writeback-Claim | Reichweite auf weitere Guardrail-Vertraege ausdehnen | `P0` |
| `F07` | Lifecycle-/Status-Drift | Status- und Lifecycle-Regeln ueber Doku, Modell und Code werden konsistent verglichen. | Mittel | normalisierte Lifecycle-Claims ueber mehrere Quellen | Statusmaschinen und Sonderwerte tiefer normalisieren | `P1` |
| `F08` | Kettenbruch in fachlichen Objektpfaden | Evidence- und Ableitungsketten ueber mehrere Objekte koennen als Hauptpfad oder Bruch nachgewiesen werden. | Mittel | Hop-Kette ueber mehrere Claims, Relationen und Quellen | explizite Ketten-Claims und Hop-Beweise weiter in Richtung Mehr-Hop- und Bruchlogik ausbauen | `P0` |
| `F09` | Temporale Luecke / Eventual Consistency | Der Auditor erkennt belastbar, wenn fachliche Konsistenz nur zeitversetzt entsteht oder kurzzeitig bricht. | Mittel | Sequenznachweis fuer Persistenz, Enqueue, Reaggregation oder asynchronen Folgepfad | temporale Claims jetzt weiter von Heuristik auf explizite Sequenzbelege umstellen | `P0` |
| `F10` | Supersede-/Refine-/Rebuild-Luecke | Aktive Kettenbrueche durch Historisierung, Rebuild oder Refine werden mit Ursache und Wirkung erkannt. | Mittel | Nachweis der Reihenfolge und des aktiven Zwischenzustands | Sequenz- und Zustandsmodell forensisch staerker machen | `P0` |
| `F11` | Feld-Propagation-/Schema-Vollstaendigkeitsfehler | Fehlende Pflichtfelder oder verlorene Zuordnungen werden ueber Pfade hinweg erkannt. | Mittel | Quellfeld, Zielerwartung und fehlende Weitergabe muessen gekoppelt nachgewiesen sein | Propagation-Claims weiter von Kontext-Heuristik auf Pfad- und Zielnachweise heben | `P0` |
| `F12` | Legacy-/Nebenpfad schwaecher als Hauptpfad | Mehrere fachlich gleiche Pfade werden verglichen und der schwaechere Pfad wird sichtbar. | Mittel | belastbarer Hauptpfad, belastbarer Nebenpfad und fachliche Vergleichsbasis | Pfadfamilien und Variantenvergleich weiter verallgemeinern | `P1` |

## Einordnung der Prioritaeten

- `P0`: Muss vor einer ehrlichen Freigabe fuer forensische Nutzung belastbar sein.
- `P1`: Muss kurz danach folgen, ist aber nicht fuer jeden Pilotfall blocking.

## Stand des Referenzkorpus

- `F01`, `F02`, `F03`, `F04`, `F05`, `F06`, `F08`, `F09`, `F10`, `F11`:
  als ausfuehrbare Positiv-/Negativfaelle im Referenzkorpus hinterlegt
- `F07`: als ausfuehrbare Faelle fuer Doku-intern, Doku-gegen-Code und
  Doku-gegen-Metamodell hinterlegt
- `F12`: als ausfuehrbarer Positiv-/Negativraum hinterlegt; umfasst jetzt
  doc-/zielpfadgestuetzten Vergleich, explizit markierte Pfadrollen,
  Mehrprimärpfade, Varianten gleicher Rolle, unmarkierte Driftfaelle sowie
  adapter-/driver-/injection-basierte Familienfaelle; weitere Verallgemeinerung
  bleibt sinnvoll, ist aber nicht mehr blocking fuer den Referenzstatus
- `F09`, `F10`, `F11`: die Risiko-Detektion ist jetzt nicht mehr nur heuristisch,
  sondern zusaetzlich mit expliziten Support-Claims fuer Sequenz- und
  Propagationsevidenz unterlegt; die Finding-Logik nutzt diese Belege jetzt
  nicht mehr nur als Wertelisten, sondern bereits als segmentierte
  Pfadmetadaten (`observed_sequence_path`, `missing_sequence_segments`,
  `sequence_rejoin_at`, `propagation_path`, `missing_field_segments`,
  `matched_propagation_variants`); kommentierte Pseudocalls und kommentierte
  Manual-/Statement-Hinweise werden fuer diese Klassen inzwischen explizit als
  Rauschen behandelt
- `F08`: explizite Hop-Claims fuer `DERIVED_FROM` und `SUPPORTS` sind jetzt
  vorhanden und werden im semantischen Graphen als Evidenzbeziehungen zwischen
  den beteiligten Objekten materialisiert; ein erster Bruchtyp ist jetzt
  als kleine Bruchfamilie abgedeckt: fehlender `Statement.DERIVED_FROM`- oder
  `Statement.SUPPORTS`-Hop; zusaetzlich existieren explizite
  `evidence_chain_path`-Claims; diese Befunde liefern inzwischen auch
  `observed_chain_path`, `expected_chain_path`, `observed_chain_variants`,
  `expected_chain_variants` und `chain_break_at`; ausserdem gibt es jetzt einen
  eigenen Konflikttyp fuer parallele aktive summary- und unit-zentrierte
  Kettenvarianten sowie erste Konflikte zwischen dokumentiertem Aktivpfad und
  implementiertem Aktivpfad; neu hinzugekommen ist eine vorsichtige
  Vollketten-Ebene (`EvidenceChain.full_path`) fuer dokumentierte/PUML-basierte
  Vollketten und konservativ inferierte Implementierungsketten; weiter offen
  bleibt die allgemeine Mehr-Hop- und Varianten-Bruchlogik; gleichzeitig
  tragen Bruchbefunde jetzt bereits Segment- und Variantenmetadaten wie
  `chain_break_mode`, `missing_chain_segments`, `chain_rejoin_at` und
  `matched_break_variants`, waehrend Vollketten-Konflikte jetzt zusaetzlich
  `full_path_divergence_mode`, `full_path_documented_gap_segments` und
  `full_path_implemented_gap_segments` fuehren; dadurch koennen mehrere
  unvollstaendige oder asymmetrische Vollkettenvarianten in einem Finding
  zusammengefasst und typisiert werden

## Aktuelle Hauptengpaesse

Die Architektur ist fuer forensische Reifung geeignet, aber die Schwaechen liegen
nicht primaer in API oder UI, sondern in drei Knotenpunkten:

1. [claim_extractor.py](/Users/martinwaelter/GitHub/FIN-AI-Auditor/src/fin_ai_auditor/services/claim_extractor.py)
   Die Extraktion ist breit und leistungsfaehig, aber fuer einige Klassen noch zu
   heuristisch und zu wenig beweisorientiert.

2. [semantic_graph_service.py](/Users/martinwaelter/GitHub/FIN-AI-Auditor/src/fin_ai_auditor/services/semantic_graph_service.py)
   Der semantische Graph ist heute vor allem Kontextgraph. Fuer tiefe Forensik
   braucht er staerkere Ketten- und Pfadbeweise.

3. [finding_engine.py](/Users/martinwaelter/GitHub/FIN-AI-Auditor/src/fin_ai_auditor/services/finding_engine.py)
   Die Finding-Engine ist heute stark fuer Widerspruchslogik, aber noch nicht
   explizit genug fuer temporale Luecken, Kettenbrueche und Pfadvergleiche.

## Konsequenz fuer die weitere Arbeit

Der Auditor darf fuer die naechsten Ausbauschritte nicht mehr primar nach
Featurebreite optimiert werden. Die Reihenfolge muss jetzt sein:

1. Klassen `F01` bis `F12` als Referenzrahmen fixieren
2. Referenzfaelle pro Klasse aufbauen
3. Claim- und Semantikschicht gegen diese Klassen haerten
4. Finding-Engine explizit auf diese Klassen ausrichten
5. Precision und Recall pro Klasse messen
