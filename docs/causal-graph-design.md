# Causal Graph Design

## Ziel

Der Causal-Graph modelliert nicht nur semantische Naehe, sondern die fachliche
Wirkungskette dafuer, wie in FIN-AI Entscheidungen entstehen und was am Ende in
die Kundendatenbank geschrieben wird.

Er beantwortet damit drei Fragen:

1. Welcher Pfad entscheidet fachlich, ob etwas geschrieben wird?
2. Welche Guardrails, Policies und Truths governen diesen Pfad?
3. Welche benachbarten Knoten muessen bei einer bestaetigten Wahrheit oder bei
   einem Delta neu bewertet werden?

## Kernobjekte

Die konkrete Datenstruktur liegt in
[causal_graph_models.py](/Users/martinwaelter/GitHub/FIN-AI-Auditor/src/fin_ai_auditor/services/causal_graph_models.py).

Sie besteht aus:

- `CausalGraphNode`: fachlicher oder technischer Wirkungsknoten
- `CausalGraphEdge`: gerichtete Abhaengigkeit oder Write-/Gate-Beziehung
- `CausalGraphTruthBinding`: bindet explizite Wahrheit an einen Knoten
- `CausalPropagationFrame`: beschreibt, wie eine Wahrheit oder ein Delta
  entlang des Graphen propagiert
- `CausalGraphEvidenceRef`: verankert Doku-/Code-/Metamodel-Evidenz an Knoten
  und Kanten

## Knotentypen

Die ersten produktiven Knotentypen sind absichtlich write-zentriert:

- `agent`, `worker`, `service`, `api_route`
- `write_contract`, `read_contract`, `policy`, `lifecycle`
- `artifact`, `relationship`, `persistence_target`
- `scope`, `phase_scope`, `truth`
- `document_anchor`, `code_anchor`

Damit lassen sich die fachlich relevanten FIN-AI-Pfade direkt abbilden:

- A3B entscheidet, welche `Statement` und `BSM_Element` entstehen
- A4 entscheidet, welche Relationships und Modellfolgen geschrieben werden
- `write_contract`, `policy` und `lifecycle` sind nicht nur Doku, sondern
  first-class Governing-Knoten
- `persistence_target` modelliert explizit den Kundengraph bzw. die
  Zielobjekte im Speicherpfad

## Kantentypen

Die initialen Kanten fokussieren auf Wahrheit, Write-Relevanz und Root Cause:

- `decides_write`: dieser Knoten entscheidet, ob/was geschrieben wird
- `writes_to`: schreibt in ein Zielobjekt oder Persistenzziel
- `gated_by`: Policy oder Guardrail blockiert/freigibt
- `feeds`, `materializes`, `derived_from`: Pipeline-Folgebeziehungen
- `depends_on`, `triggered_by`, `invalidates`: Delta- und Recompute-Ketten
- `propagates_truth_to`: explizite Wahrheit muss hierhin gespiegelt werden
- `documents`, `implemented_by`, `evidenced_by`: Evidenzanker

Jede Kante traegt zusaetzlich:

- `propagation_mode`: `none`, `truth_only`, `delta_only`, `truth_and_delta`
- `strength`: Relevanz und Traversal-Gewicht
- `blocking`: ob die Kante als harter Gate wirkt
- `write_relevant` und `truth_relevant`

## Beispiel fuer FIN-AI

Ein erster produktiver Graph fuer den BSM-Pfad sollte mindestens diese
Wirkungskette tragen:

1. `A3B` `feeds` `summarisedAnswerUnit`
2. `summarisedAnswerUnit` `materializes` `Statement`
3. `Statement.write_contract` `gated_by` `Statement.policy`
4. `A3B` `decides_write` `Statement`
5. `Statement` `feeds` `BSM_Element`
6. `A4` `decides_write` `Relationship`
7. `BSM_Element` und `Relationship` `writes_to` `persistence_target`
8. explizite Wahrheit bindet ueber `CausalGraphTruthBinding` an
   `Statement.write_contract`, `Statement.policy` oder `Relationship.lifecycle`

## Warum diese Struktur besser ist

Die heutige Kausal-Attribution traversiert den semantischen Graphen breit und
leichtgewichtig. Der neue Causal-Graph ist enger und fachlich gerichteter:

- er unterscheidet Governing-Knoten von Runtime-Knoten
- er modelliert Write-Relevanz explizit statt nur semantisch
- er traegt Truth- und Delta-Propagation als Daten, nicht als Heuristik
- er ist auf Root-Cause-Bildung fuer A3B, A4, Chunking, Mining und
  Persistenzpfade zugeschnitten

## Implementierter Stand

Die semantische Graph-Schicht bleibt die Quelle fuer den Graph-Bau, aber der
Builder erzeugt jetzt bereits produktiv:

- `write_decider`-Knoten aus write-relevanten Code-Claims und Section-Pfaden
- `persistence_target`-Knoten fuer die fachlichen Zielobjekte im
  Kundengraphen
- `decides_write`, `implemented_by`, `writes_to`, `materializes` und
  `triggered_by`-Kanten zwischen Contract, Decider, Runtime-Code und Sink

Damit laeuft die Root-Cause-Attribution jetzt nicht mehr nur bis zum
Write-Contract, sondern bis zum fachlichen Write-Entscheider und zum
Persistenzziel.

## Naechster Ausbauschritt

Die verbleibende Ausbaustufe waere eine noch feinere Sink-Modellierung:

- echte Unterscheidung zwischen Node-, Relationship- und History-Write-Zielen
- explizite Mapping-Knoten fuer Chunking-, Mining- und Agent-Persistenzpfade
- direkte Verknuepfung mit den konkreten Customer-DB-Write-APIs statt nur mit
  den semantisch erkannten Write-Claims
