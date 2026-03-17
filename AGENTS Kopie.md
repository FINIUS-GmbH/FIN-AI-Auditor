# AGENTS.md

Diese Datei definiert projektlokale Arbeitsregeln und Guardrails fuer Codex- und Agentenarbeit in diesem Repository.

## Arbeitsstandard

- Nicht bei `technisch verbessert`, `Patch erstellt` oder `Tests gruen` stoppen, wenn die eigentliche fachliche Aufgabe noch nicht Ende-zu-Ende nachgewiesen ist.
- `Naechste empfohlene Schritte` nur dann als Abschluss nennen, wenn der angefragte Scope wirklich erledigt ist oder ein echter externer Blocker verbleibt.
- Wenn der User eine Luecke, einen False Negative oder einen nicht gefundenen Befund zeigt, gilt die Aufgabe erst dann als sauber abgeschlossen, wenn die konkrete Befundklasse reproduzierbar erkannt oder der verbleibende Blocker hart belegt ist.

## Definition of Done

Eine Analyse-, Erkennungs- oder Detector-Aufgabe gilt erst dann als abgeschlossen, wenn alle folgenden Punkte erfuellt sind:

1. Der reale oder synthetisch belastbare Referenzfall ist als Test oder reproduzierbarer Nachweis abgebildet.
2. Der Pfad `Quelle -> Claim/Signal -> Detektor -> Finding/Output` ist geschlossen und verifiziert.
3. Die Ausgabe ist fachlich brauchbar und nicht nur technisch vorhanden.
4. Bekannte Restluecken derselben Befundklasse sind explizit geprueft und entweder geschlossen oder als harter Blocker dokumentiert.
5. Abschlusskommunikation behauptet nicht `fertig`, wenn nur ein Zwischenstand erreicht wurde.

## Guardrails Fuer Antworten

- Nicht vorschnell mit Empfehlungen fuer spaetere Arbeit ausweichen, wenn die angeforderte Umsetzung im aktuellen Turn noch machbar ist.
- Keine beruhigenden Formulierungen wie `ist jetzt deutlich besser` als Ersatz fuer einen echten Ende-zu-Ende-Nachweis verwenden.
- Bei qualitativen Aussagen wie `vollstaendig`, `sauber abgeschlossen`, `produktbereit` oder `erkennt jetzt` immer einen konkreten Nachweis nennen.
- Wenn noch Restarbeit bleibt, klar zwischen `erledigt`, `teilweise erledigt` und `nicht erledigt` trennen.
- Keine generischen Fazitsaetze wie `der Gesamtzustand ist jetzt klar besser` oder `deutlich belastbarer als vorher` verwenden, wenn nicht direkt dazu gesagt wird:
  - was konkret verbessert wurde,
  - gegen welchen vorherigen Stand verglichen wird,
  - und welches praktische Delta daraus folgt.
- Keine allgemeinen Bewertungen wie `gut`, `schlecht`, `stark`, `schwaecher` als Standardabschluss verwenden. Solche Einordnungen nur nennen, wenn sie fuer die Entscheidung wirklich relevant sind.

## Guardrails Fuer Fazits Und Statusmeldungen

- Jede Abschlussantwort muss sich primaer auf den aktiven Gesamtplan oder die Gesamtfertigstellung beziehen, nicht auf triviale Einzelabschnitte.
- Prozentangaben sollen standardmaessig den Stand der Gesamtfertigstellung ausdruecken:
  - Wie weit ist der gesamte Zielplan erledigt?
  - Wie weit ist die Gesamtstrecke bis zum aktuellen Zielzustand abgeschlossen?
- Prozent fuer einen Einzelblock nur dann nennen, wenn der User explizit nach diesem Block fragt oder wenn mehrere grosse Bloecke parallel laufen und der Blockstand fuer die Entscheidung relevant ist.
- Keine no-brainer-Angaben wie `100 % fuer diesen Regelblock`, wenn die eigentlich relevante Frage die Gesamtfertigstellung ist.
- Fuer den Abschluss standardmaessig immer explizit angeben:
  1. `Gesamtfortschritt`: in Prozent fuer den aktiven Gesamtplan
  2. `Delta zum vorherigen Stand`: was jetzt konkret neu geht
  3. `Noch nicht moeglich`: was weiterhin nicht geht oder nicht sauber abgedeckt ist
  4. `Naechster grosser Block`: was als naechstes logisch folgt
- Prozentangaben duerfen nicht frei formuliert werden. Immer klar sagen, wovon der Prozentwert handelt:
  - Prozent der Gesamtstrecke
  - oder ausnahmsweise Prozent eines Teilplans, wenn genau dieser gemeint ist
- Bei Delta-Aussagen immer den Vergleichsrahmen nennen:
  - `vor diesem Block`
  - `gegen den letzten verifizierten Stand`
  - `gegen den bisherigen Referenzkorpus`
- Abschlussantworten muessen explizit die drei Listen enthalten:
  - `Neu moeglich`
  - `Noch nicht moeglich`
  - `Als naechstes`
- Nicht jedes Mal wiederholen, was allgemein gut oder schlecht ist, wenn sich daran gegenueber dem vorherigen Stand nichts geaendert hat.

## Prioritaet Bei Erkennungsproblemen

Wenn ein Befund nicht gefunden wird, ist in dieser Reihenfolge zu pruefen:

1. Kommt das Quellsignal ueberhaupt in die Pipeline?
2. Wird daraus ein korrekt normalisierter Claim oder ein gleichwertiges Signal?
3. Vergleicht der zuständige Detector die richtigen Konzepte gegeneinander?
4. Werden Single-Source-Risiken faelschlich weggefiltert, obwohl kein Cross-Source-Konflikt noetig ist?
5. Existiert ein Test, der genau diese Befundklasse absichert?

## Abschlussformat

- Am Ende immer klar sagen: Was wurde gemacht, was ist jetzt nachweislich erreicht, was ist noch offen.
- `Empfohlene naechste Schritte` nur als separater Zusatz nach einem ehrlichen Status, nicht als Ersatz fuer fehlende Umsetzung.
- Das Fazit muss primaer den Gesamtfortschritt des aktiven Zielplans benennen.
- Das Fazit muss den Unterschied zum vorherigen Stand benennen, nicht nur den aktuellen Stand.
- Das Fazit soll bevorzugt diese Struktur nutzen:
  - `Gesamtfortschritt`
  - `Vergleich zum vorherigen Stand`
  - `Neu moeglich`
  - `Noch nicht moeglich`
  - `Naechster grosser Block`
