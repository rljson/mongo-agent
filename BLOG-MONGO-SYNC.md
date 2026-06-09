Verteilte Datenbanken brauchen nicht nur einen sauberen Sync-Pfad, sondern auch belastbare Beweise, dass sie nach realen Störungen wieder konvergieren. Genau dafür ist jetzt eine vollständige bidirektionale MongoDB-Sync-Plattform entstanden – inklusive Konfliktauflösung, Selbstheilung über Blockchain-Replay und einem Operations-Dashboard im Browser.

## Motivation

Im vorherigen Schritt ging es um die grundsätzliche Idee, MongoDB-Daten zwischen mehreren Standorten lebendig zu halten: Change-Streams, eine zentrale Hub-Relay-Komponente, sequenzielle Operations-Ströme. Der nächste Schritt ist zwingend: Ein solches System muss nicht nur konzeptionell funktionieren, sondern auch dann sauber bleiben, wenn beide Seiten gleichzeitig schreiben, ein Knoten kurz weg ist, jemand am Filesystem rumdreht oder eine ganze Collection wegbricht.

Genau dafür ist jetzt eine produktionsnahe Sync-Schicht aufgebaut worden. Ziel ist es, nicht nur den Happy Path eines Inserts zu prüfen, sondern alle realen Drift-Klassen – Konflikte, Datenverlust, Hash-Drift, Pre-Chain-Damage – automatisch erkennen, klar darstellen und reparieren zu können.

## Strategie

* **Bit-genau konvergieren statt „ungefähr gleich"**: Beide Datenbanken müssen am Ende denselben `dbRoot`-Hash haben – sonst ist der Sync nicht fertig.
* **Typ-Agnostik**: Date, ObjectId, Decimal128, Binary – alles muss über die Leitung überleben, ohne dass irgendein Code-Pfad einen Typkonverter braucht.
* **Konflikte ehrlich machen**: Concurrent-Update, Update-vs-Delete und Concurrent-Insert werden alle drei explizit erkannt, unterschieden und in einer eigenen Collection sichtbar.
* **Selbstheilung explizit testen**: Damaged-DB, fehlende Docs, Drift im Inhalt, gebrochene Chain – jedes Szenario hat einen eigenen Reparaturpfad.
* **Bedienbarkeit sichtbar machen**: Reports, Hash-Status, Chain und Partitionen sollen direkt im Browser einsehbar und bedienbar sein.

## Neuigkeit

Für das CARAT-one-client-Setup gibt es jetzt eine vollständige bidirektionale MongoDB-Sync-Plattform, die auf zwei physischen Windows-Laptops im echten LAN läuft (`NB-2666` ↔ `NB-2750` über einen Hub auf Port 3200). Sie umfasst typsichere Replikation, dreiklassige Konflikterkennung mit propagierter Auflösung, Selbstheilung über Blockchain-Replay, Peer-Restore für Pre-Chain-Daten, einen Per-Origin-Chain-Integritäts-Check und eine 414× schnellere inkrementelle State-Hash-Berechnung.

Zusätzlich gibt es ein Angular-Dashboard, über das sich Hub und Agents starten und stoppen lassen, der aktuelle Hash-Status pro Collection einsehbar ist, die Sync-Chain visuell durchwanderbar wird und Konflikte direkt im Browser aufgelöst werden können.

![CARAT one client Dashboard – This Node, Network, Sync Agents, Alerts, Quick Stats](file:///C:/Users/mertke.extern/Downloads/client3.png)

## Details

### 1) Zwei getrennte Kanäle: Sync-Stream und Steuerung bewusst entkoppelt

Der wichtigste Architekturpunkt ist die Trennung in zwei getrennte HTTP-Wege:

**Sync-Kanal**
Der eigentliche Datenpfad zwischen den Knoten geht über einen zentralen Hub. Jeder Agent (L1 = `NB-2666:3001`, L2 = `NB-2750:3002`) liest seinen lokalen MongoDB-Change-Stream, schreibt jede Operation als Eintrag in `sync_ops`, und zieht über den Hub die Operationen der anderen Seite.

**Control-Kanal**
Eine separate Conflict-API auf Port 3000 stellt alle Operations-Endpoints bereit: `services/{status,start,stop}`, `repair/{restore-from-chain,restore-from-peer,backfill-hashes}`, `hash-status`, `chain/{origins,verify}`, `partitions`. Über diesen Kanal werden auch die Konflikte aus `sync_conflicts` gelesen und aufgelöst.

Diese Trennung ist entscheidend. Wenn der Sync klemmt, bleibt der Control-Kanal bedienbar – und der Operator kann weiter den Hash-Status abfragen, die Chain inspizieren oder einen Repair-Lauf anstoßen.

### 2) Service-Steuerung direkt aus dem Dashboard

Statt PowerShell-Skripte über drei Maschinen zu jagen, lässt sich der gesamte Stack jetzt zentral aus dem Dashboard starten und stoppen. Hub und beide Agents zeigen Live-Status (`UP`/`DOWN`), und der L2-Agent läuft per WinRM mit `Win32_Process.Create` als wirklich detached Prozess auf dem Remote-Laptop – er überlebt also das Schließen der WinRM-Session.

![Service Control mit Start/Stop für Hub, Agent L1 und Agent L2 (remote) sowie Repair-Aktionen](file:///C:/Users/mertke.extern/Downloads/client1.png)

### 3) Typ-Agnostischer Sync via EJSON

Ein früher Bug war, dass `Date`-Felder nach dem ersten Sync auf der Empfängerseite plötzlich als ISO-String ankamen. Ursache war ein zu aggressives `JSON.parse(JSON.stringify(...))` im Apply-Pfad, das alle BSON-Typen plattgemacht hat.

Die Lösung war kein typspezifischer Walker, sondern ein klarer Schnitt: **alles, was über die Leitung geht, ist EJSON**. Auf der Sender-Seite wird `EJSON.serialize` direkt vor dem HTTP-Body verwendet, auf der Empfänger-Seite spiegelt `EJSON.deserialize` die nativen BSON-Typen wieder zurück. Damit überleben Date, ObjectId, Decimal128 und Binary den Sync ohne dass irgendein Code-Pfad sie kennen muss.

### 4) Per-Doc Integritätshash (`__h`)

Jedes synchronisierte Dokument bekommt einen Integritätshash `__h`, berechnet via `computeIntegrityHash` über `stableStringify(doc)`. Dieser Hash hat zwei Aufgaben:

* **Schneller Drift-Check**: Beim Self-Heal-Walk wird nicht der ganze Doc-Inhalt verglichen, sondern nur `__h`. Ein einziger Lookup, ein Hash-Vergleich – das spart bei Millionen Docs Stunden.
* **Schneller State-Hash**: Wenn die Sample-Probe zeigt, dass `__h` flächendeckend vorhanden ist, projiziert die State-Hash-Berechnung nur `{_id, __h}` und überspringt den teuren `stableStringify`-Walk komplett.

### 5) Inkrementelle State-Hash-Berechnung mit Merkle-Bäumen

Der `dbRoot`-Hash ist die Wahrheit. Wenn er auf beiden Laptops gleich ist, sind die Datenbanken bit-identisch konvergiert. Naiv jedes Mal alle Docs durchzuwalken kostet Minuten, deshalb ist der Hash hierarchisch:

* Pro Collection wird in **Partitionen à 50.000 Dokumente** gehasht (`state_merkle`).
* Jede Partition hat einen eigenen Hash.
* Eine geänderte Partition wird in `state_dirty` markiert (`markDirtyById`) und beim nächsten Lauf einzeln neu berechnet.
* Der Collection-Root wird aus den Partitions-Hashes aggregiert, der `dbRoot` aus den Collection-Roots.

Das Ergebnis pro Collection ist im Hash-Status-Tab direkt sichtbar – inklusive Partitionsanzahl und Root-Hash.

![Hash Status pro Collection mit Partitionsanzahl und Root-Hash](file:///C:/Users/mertke.extern/Downloads/client2.png)

### 6) Performance: 414× Beschleunigung des inkrementellen Hash-Laufs

Der inkrementelle Hash-Lauf war ursprünglich >84 Sekunden auf der `cd_models`-Collection. Drei aufeinander aufbauende Optimierungen haben ihn auf weniger als 200 ms gedrückt:

1. **Sample-basierte Eligibility**: Statt mit `findOne({__h: {$exists: false}})` einen Full-Scan zu triggern, entscheidet jetzt ein einziges `findOne({}, {projection: {__h:1}, sort: {_id: 1}})` pro Collection, ob `__h`-Projektion sicher ist.
2. **`{_id, __h}`-Projection-Path**: Wenn die Sample-Probe positiv ist, wird der teure `stableStringify`-Walk komplett übersprungen.
3. **`lastIdx`-Nachzügler-Erkennung**: Append-Inserts hinter den maximalen `_id` der letzten Partition werden korrekt als „letzte Partition dirty" markiert, statt einen Full-Rebuild zu triggern.

Effekt: ein vollständiger inkrementeller Lauf nach einer kleinen Änderung dauert jetzt weniger als eine halbe Sekunde – auch auf Collections mit Millionen Docs.

### 7) Konflikterkennung: alle drei Klassen explizit

Konflikte werden jetzt in drei klar getrennten Klassen erkannt und in `sync_conflicts` festgeschrieben:

* **`concurrent-update`** – beide Seiten editieren denselben Doc parallel; erkannt anhand divergierender Leaf-Hashes mit jeweils eigenem `prevHash`.
* **`update-delete`** – Case A: lokal wurde gelöscht, der Peer hat aber später noch geupdated. Case B: lokal wurde geupdated, der Peer hat parallel gelöscht. Beide Richtungen werden in `pull-from-hub.ts` separat detektiert, einmal im Insert/Update-Branch und einmal im Delete-Branch.
* **`concurrent-insert`** – beide Seiten legen denselben `_id` mit unterschiedlichem Inhalt an.

Im UI sind die Konflikte nach Typ farblich unterschieden, mit `Document ID`, `Collection`, `Detected at` und allen beteiligten Versionen pro Knoten.

![Conflicts-Übersicht mit Update vs Delete und Concurrent Updates pro Doc](file:///C:/Users/mertke.extern/Downloads/client8.png)

### 8) Resolution-Propagation

Der frühere Bruch war: Operator wählt im UI „use-local", die andere Seite hat aber davon nichts gehört und behält ihren Stand – die Datenbanken konvergieren nicht.

Jetzt schreibt der Resolver die Auflösung in einen `pendingConflict`-Eintrag, den der Empfänger beim nächsten Pull liest. Auf der Empfängerseite wird die Auflösung sowohl im Insert/Update- als auch im Delete-Branch berücksichtigt. Wer auf Delete-Seite resolved, dessen Delete-Op wird auf der anderen Seite **nicht** als neuer Update-vs-Delete-Konflikt re-flagged, sondern als „aufgelöst durchreichen" behandelt.

Saubere Trennung: keine Resolution-Metadaten am Customer-Doc, kein `_resolvedFrom`/`_resolvedAt`-Feldsalat – Auflösungs-Audit kommt später in eine eigene Collection.

### 9) Selbstheilung über Blockchain-Replay

`src/scripts/restore-from-chain.ts` walkt die `sync_ops`-Collection in Sequenz-Reihenfolge und entscheidet pro Operation:

* **Insert/Update/Replace**: Existiert der Doc lokal? Wenn nein → restore. Wenn ja, weicht der Leaf-Hash ab? → restore.
* **Delete**: Existiert der Doc lokal noch? → re-delete.

Per-Origin Chain-Integrity wird über eine `Map<origin, prevHash>` validiert, sodass Multi-Origin-Chains keine falschen Brüche melden. `DRY_RUN=1` zeigt zuerst, was passieren würde, ohne etwas zu tun. Nach erfolgreicher Reparatur werden die wiederhergestellten Docs automatisch via `markDirtyById` in `state_dirty` markiert, sodass der nächste Hash-Lauf sie auch tatsächlich aufnimmt.

### 10) Peer-Restore für Pre-Chain-Daten

Self-Heal aus der Chain repariert nur Daten, die nach dem Chain-Start passiert sind. Was vorher per `mongorestore` reingekommen ist, lebt unterhalb von Sequenz 1 – wenn das verloren geht, hilft die Chain nicht.

Dafür gibt es jetzt `src/scripts/restore-from-peer.ts`. Das Skript:

* Holt sich vom Peer-Agent die `_id`-Liste der Collection (`POST /diff/:coll/ids`),
* gleicht sie mit dem lokalen `_id`-Set ab,
* zieht für jede fehlende `_id` per `POST /diff/:coll/doc` den Doc als EJSON,
* und schreibt ihn lokal ein – mit korrekt restaurierten BSON-Typen.

`keyOf(id)` macht dabei den Set-Vergleich stabil über ObjectId/String/UUID hinweg.

### 11) Chain-Inspector: die Operations-Wahrheit im Browser

Die `sync_ops`-Chain ist im Dashboard direkt durchwanderbar. Pro Origin lassen sich alle Operationen mit `seq`, `op`, `coll`, `docId`, `prev → chain` und Timestamp ansehen. Ein „Verify chain"-Button rechnet die Per-Origin-Hash-Kette einmal komplett durch – gebrochene Glieder werden im UI rot hervorgehoben.

![Chain Inspector mit seq, op, coll, docId, prev → chain und Timestamps](file:///C:/Users/mertke.extern/Downloads/client4.png)

### 12) Repair-Bedienkonsole im Dashboard

Die drei Reparaturwerkzeuge sind direkt im Dashboard als Cards verfügbar:

* **Self-heal from chain** – mit `DRY_RUN`- und `Apply`-Knopf
* **Restore from peer (laptop2)** – Collection-Eingabefeld plus `Run`
* **Backfill `__h` hashes** – um Pre-`__h`-Daten nachträglich auf den schnellen Projection-Pfad zu bringen

Damit sind alle Wartungsfälle, die früher ein PowerShell-Skript erfordert haben, jetzt One-Click-Operationen.

### 13) Sync-Agent-Sicht und Konnektivität

Die Sync-Seite des Dashboards zeigt pro Agent: MongoDB-URI, Tree Key, Verbindungsstatus, Last Sync, Last Seq, Last Hash und welche Datenbanken der Agent gerade synchronisiert. Eine „Recent Activity"-Liste zeigt die letzten Operations mit Ziel-Collection, Op-Typ und Sequenznummer.

![Sync Agents – Status, Verbindung, Last Sync, Recent Activity](file:///C:/Users/mertke.extern/Downloads/client7.png)

### 14) Network-Topologie und Probe-Ergebnisse

Damit klar ist, wer mit wem spricht, gibt es eine Topology-Map: jeder Knoten als Karte mit Rolle (`HUB`/`CLIENT`), IP, Port und Node-ID. Network-Actions erlauben Hub-Override und Election-Exclusion direkt aus dem UI.

![Network Topology – 3 Knoten, Rollen, Network Actions](file:///C:/Users/mertke.extern/Downloads/client5.png)

Eine zusätzliche Probe-Results-Sektion misst Latenz pro Peer in Echtzeit und zeigt, welche Discovery-Layer aktiv sind (Broadcast, Cloud, Static, Manual).

![Probe Results und Discovery Layers](file:///C:/Users/mertke.extern/Downloads/client6.png)

### 15) Test-Coverage für die neuen Pfade

Die neue Logik ist vollständig durch Unit-Tests abgedeckt:

* **`pull-from-hub.spec.ts`**: 26/26 grün, mit dedizierten Tests für concurrent-update, resolution-propagation, update-delete Case A (Peer löscht lokal-editiert) und Case B (Peer editiert lokal-gelöscht).
* **`state-dirty.spec.ts`**: 16/16 grün, inklusive der drei neuen Tests für den After-Max-Branch der Partitions-Markierung.
* **`state-hash.spec.ts`**: angepasst auf das neue Sample-Probe-Mocking.

### 16) Validierte Use-Cases

Folgende Szenarien sind End-to-End am realen 2-Laptop-Setup verifiziert worden:

* Bulk-Restore aus identischem `mongorestore`-Dump auf beide Seiten → identischer `dbRoot`
* 20 Customer-Inserts auf L1 → in <2 s auf L2 sichtbar, `dbRoot` wieder identisch
* `Date`-Feld bleibt typgenau (kein String-Drift mehr)
* Concurrent-Update auf demselben Customer → Konflikt in `sync_conflicts`, im UI sichtbar
* Concurrent-Insert mit gleichem `_id` und unterschiedlichem Body → Konflikt erkannt
* Update-vs-Delete in beide Richtungen → Konflikt erkannt, im UI mit „Document deleted on this node"-Platzhalter
* Konflikt-Auflösung im UI → propagiert zur Gegenseite, beide `dbRoots` konvergieren wieder
* Damaged-DB-Self-Heal: Doc gelöscht → Self-heal-from-chain stellt ihn wieder her
* Damaged-DB-Self-Heal: Doc-Inhalt verändert → Drift erkannt, Restore aus Chain-Payload
* Re-Delete-Branch: Doc nach Delete-Op nochmal lokal vorhanden → korrekt re-deleted
* Pre-Chain-Damage: Customer-Collection gewiped → Restore-from-Peer stellt alle Pre-Chain-Daten wieder her
* Chain-Integrity-Violation: gebrochener Hash-Link → Verify-Chain-Endpoint markiert genau die betroffenen Sequenzen
* Service-Restart aus dem Dashboard: Hub stop/start, L1 stop/start, L2 stop/start (remote über WinRM)

### 17) Architekturentscheidungen

* **Bewusst kein Resolution-Metadata am Doc**: kein `_resolvedFrom`, kein `_resolvedAt`, kein `_resolvedBy` am Customer. Falls Audit-Daten gebraucht werden, gehören sie in eine eigene Collection.
* **Per-Origin Chain statt globaler Chain**: ein einziger `prevChain` für alle Origins erzeugt False-Positives in Multi-Origin-Setups. Die `Map<origin, prevHash>` löst das sauber.
* **EJSON Ende-zu-Ende**: kein Typkonverter, keine Sonderfälle pro BSON-Typ.
* **Suppressor-Pattern statt Echo-Filter**: Operations, die der Agent gerade selbst auf die DB schreibt, werden vor dem Stream-Callback gesetzt und eine Sequenz später wieder entfernt. So entstehen keine Echo-Loops.

## Zusammenfassung

| Kennzahl | Wert |
|---|---|
| Validierte Use-Cases | 13 |
| Konfliktklassen abgedeckt | 3 (concurrent-update, update-delete A+B, concurrent-insert) |
| Reparatur-Werkzeuge | 3 (chain-replay, peer-restore, backfill-`__h`) |
| Inkrementeller State-Hash – Speedup | 414× |
| Inkrementeller Hash-Lauf – Endzustand | < 200 ms |
| Unit-Test-Suiten grün | `pull-from-hub` 26/26, `state-dirty` 16/16 |
| Physische Test-Knoten | 2 Laptops + 1 Hub |
| Datenvolumen `CARATDB` | 1051 MB, 37 Collections |
| Sync-Latenz Insert→Sichtbarkeit | < 2 s |

Damit deckt die Plattform den gesamten Weg von der lokalen Mutation über die typsichere Replikation bis zur Konfliktauflösung und Selbstheilung ab – inklusive bedienbarem Operations-Dashboard im Browser.

## Nächster Schritt

* Resolution-Audit-Collection einführen, damit Auflösungs-Historie nachvollziehbar bleibt, ohne den Doc selbst zu polluten
* Automatischer Chain-Verify-Lauf als Hintergrundjob mit Alert im Dashboard, falls Per-Origin-Hash bricht
* Restore-from-Peer als ChunkSize-fähigen Bulk-Modus für sehr große Collections
* Mehr-als-zwei-Laptop-Topologie validieren (drei Origins parallel mit Konflikten zwischen je zwei Paaren)
* End-to-End-Tests in CI als dauerhafte Qualitätsbarriere im Entwicklungsprozess

## Dank

Dank an Gatzsche, Gabriel für die Reflexion. Dank an Kirchdorfer, Anita und Schönfeld, Sascha für die Projektbegleitung.
