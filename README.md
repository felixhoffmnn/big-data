# Hubway Bike Data

Projektabgabe für die Vorlesung _Big Data_ während des 5. Semesters and der DHBW Stuttgart. Dieses Projekt beschäftigt sich mit dem Konvertieren und Auswerten von einem [Datensatz](https://www.kaggle.com/datasets/acmeyer/hubway-data) eines **Bike-Sharing** Anbieters aus **Boston**.

## Installation

Bevor der Workflow zum Konvertieren der Daten ausgeführt werden kann ist es notwendig **Hadoop** und **Airflow** zu starten. Hierfür werden die vorgegeben Container verwendet. Falls dies auf einer GCP VM ausgeführt wird, muss zunächst Docker installiert werden ([Guide](https://docs.docker.com/engine/install/ubuntu/)).

```bash
docker compose up -d
```

Nachdem die Container gestartet haben, muss erst noch **Hadoop** und **Hive** gestartet werden. Dies geschieht jedoch innerhalb des `hadoop` Containers.

```bash
docker exec -it hadoop bash
sudo su hadoop
start-all.sh
hiveserver2
```

Es ist relevant das Terminal, in welchem `hiveserver2` ausgeführt wird, nicht zu schließen. Ansonsten wird der Hive Server beendet und die Daten können nicht mehr ausgelesen werden.

Unter http://localhost:8080/admin sollte nun Airflow erreichbar sein. Hier können die einzelnen Dags ausgeführt werden.

## Konzept

Für einen leichteren Umgang verwendet dieses Projekt **Docker Compose** um die benötigten Dateien direkt an die Container zu übergeben. Dadurch werden außerdem die finalen **KPIs** auch im Dateisystem des Nutzers gespeichert.

Nach dem Ausführen des `bike_dag` werden folgende Schritte ausgeführt:

1. Erstellt oder leert ein Verzeichnis für den Download der Rohdaten
2. Installiert die benötigten Python Pakete
3. Lädt den Datensatz herunter
4. Erstellt ein Ordner für jede `yearmonth` Kombination im HDFS Filesystem
5. Kopiert die heruntergeladenen Daten in den `raw` Ordner auf dem HDFS Filesystem
6. Lädt die `raw` Daten und führt Funktionen auf diesen aus und speichert diese letztlich im `final` Ordner auf dem HDFS Filesystem
7. Lädt die `final` Daten in die Hive Datenbank
8. Berechnet die **KPIs** mittels PySpark
9. Lädt die Ergebnisse von dem HDFS Filesystem in das Dateisystem des Nutzers in den `data/output` Ordner

### Funktionen

| Funktion | Beschreibung |
| -------- | ------------ |
| Test     | Test         |

### Datenbereinigung

Um die Qualität der Daten sicherzustellen, werden die Daten vor der Berechnung der KPIs bereinigt. Hierbei werden die folgenden Schritte ausgeführt:

1. Filter `trip_duration` nach Werten größer als `0` und kleiner als `86400` (24 Stunden)
2. Filter `age` nach Werten größer als `0` und kleiner als `100`
3. Filter `generation` nach Werten größer als `0` (`generation == -1` repräsentiert einen einen Fehler)
4. Filter nach `timeslot_[0,1,2,3]` so dass mindestens ein Wert größer als `0` ist

### Task Flow

Lorem Ipsum

## Problems

1. Wenn ich die docker compose lokal ausgeführt habe, beendet der `hiveserver2` sich immer nach `30` Verbindungen. Sobald man den `hiveserver2` einfach erneut startet und den dag ausführt, läuft alles wieder.
2. In der docker compose werden einerseits Ordner wie `dags`, `python` und `output` gemounted. Bei letzterem kam es zu Problemen, da der Ordner nicht von einem lokalen Nutzer im Container erstellt wurde. Mittels dem folgenden Befehl können die notwendigen Berechtigungen angepasst werden.

    ```bash
    sudo chmod 777 data/output/
    ```

3. Wenn die docker compose auf einem Windows Rechner ausgeführt wird, kann es dazu kommen, dass die `EOL` von `LF` sich auf `CRLF` ändert. Dies führt dazu, dass der `airflow` Container nicht starten kann.
