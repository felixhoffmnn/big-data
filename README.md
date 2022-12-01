# Hubway Bike Data

| Vorlesung | Semester | Matrikelnummer | Dozent             |
| --------- | -------- | -------------- | ------------------ |
| Big Data  | WS 22/23 | 1946566        | Marcel Mittelstädt |

Projektabgabe für die Vorlesung _Big Data_ während des 5. Semesters and der DHBW Stuttgart. Dieses Projekt beschäftigt sich mit dem Konvertieren und Auswerten von einem [Datensatz](https://www.kaggle.com/datasets/acmeyer/hubway-data) eines **Bike-Sharing** Anbieters aus **Boston**.

## Installation

Um das Projekt auf Google Cloud VMs auszuführen, kann es simpler sein, das Git-Repository zu clonen. Dafür kann der folgende Befehl verwendet werden:

```bash
git@github.com:felixhoffmnn/big-data.git
```

Alternativ kann dieser Schritt auch mit `rsync` ersetzt werden. Hierfür könnte dieser [Guide](https://phoenixnap.com/kb/how-to-rsync-over-ssh) hilfreich sein.

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

Für einen leichteren Umgang verwendet dieses Projekt [Docker Compose](#docker-compose) um die benötigten Dateien direkt an die Container zu übergeben. Dadurch werden außerdem die finalen **KPIs** auch im Dateisystem des Nutzers gespeichert.

![Task Flow](./data/images/task_flow.png)

Nach dem Ausführen des `bike_dag` werden folgende Schritte ausgeführt:

1. **Erstellt** und / oder **leert** Verzeichnisse für den Download und die finalen KPIs
2. Lädt den Datensatz von _Kaggle_ herunter
3. Schaut welche Dateien heruntergeladen wurden und **filtert** diese (`year_months`)
4. Erstellt ein Ordner für jede `yearmonth` Kombination im _HDFS Filesystem_
5. Kopiert die heruntergeladenen Daten in den `raw` Ordner auf das _HDFS Filesystem_
6. **Lädt** die `raw` Daten mittels _PySpark_ und führt Funktionen auf diesen aus und speichert diese letztlich im `final` Ordner auf dem _HDFS Filesystem_
    - Test
7. **Lädt** die Daten aus `final` mittels _PySpark_ in ein Dataframe
8. Berechnet die **KPIs** mittels _PySpark_
9. **Lädt** die Ergebnisse von dem _HDFS Filesystem_ in das Dateisystem des Nutzers in dem `data/output` Ordner

### Funktionen

In diesem Abschnitt werden die Primären Funktionen der wichtigsten Dateien erklärt.

`bike_dag.py`:

| Funktion                      | Beschreibung                                                                                                                       |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `create_local_import_dir`     | Erstelle Ordner für den Download, wenn dieser nicht existiert (`/home/airflow/bike_data`)                                          |
| `create_output_dir`           | Erstelle Ordner für die kombinierten KPIs (`/home/airflow/output`)                                                                 |
| `clear_local_import_dir`      | Löscht Kaggle-Datensatz von vorherigen Durchläufen                                                                                 |
| `clear_output_dir`            | Löscht Kombinierte KPIs aus vorherigen Durchläufen                                                                                 |
| `download_data`               | Lädt den Datensatz von Kaggle herunter (`/home/airflow/bike_data`)                                                                 |
| `list_files`                  | Listet alle Dateien aus `/home/airflow/bike_data` welche mit `6` Nummern (`yyyymm`) anfangen                                       |
| `create_hdfs_partition_raw`   | Erstellt partitionierte Ordner für die `raw` Daten (`/user/hadoop/hubway_data/raw/{yyyymm}`)                                       |
| `create_hdfs_partition_final` | Erstellt partitionierte Ordner für die `final` Daten (`/user/hadoop/hubway_data/final/{yyyymm}`)                                   |
| `create_hdfs_partition_kpis`  | Erstellt partitionierte Ordner für die `kpis` Daten (`/user/hadoop/hubway_data/kpis/{yyyymm}`)                                     |
| `copy_raw_to_hdfs`            | Kopiert die `csv` Dateien von `/home/airflow/bike_data` nach `/user/hadoop/hubway_data/kpis/{yyyymm}/{yyyymm}-hubway-tripdata.csv` |
| `pyspark_calculate_kpis`      | Optimiert und bereinigt die Rohdaten auf dem HDFS und verschiebt die `csv` Dateien von `raw` nach `final`                          |
| `pyspark_combine_kpis`        | Fasst die die partitionierten KPIs in einer Datei zusammen und speichert diese in den `output` Ordner                              |

`calculate_kpis.py`:

| Funktion              | Beschreibung                                                                                 |
| --------------------- | -------------------------------------------------------------------------------------------- |
| `get_distance`        | Berechnet die Distanz zwischen zwei Punkten auf der Basis von Latitude und Longitude         |
| `get_age`             | Berechnet das Alter eines Nutzers anhand des Geburtsjahres und des aktuellen Datums          |
| `get_timeslot_helper` | Gibt einen Zeitslot zwischen 0 oder 3 zurück.                                                |
| `get_timeslot`        | Berechnet den Zeitslot eines Datums basiert auf einem Start- und Enddatum und einem Zeitslot |
| `get_generation`      | Gibt die Generation eines Nutzers zwischen 0, 1, 2, 3, 4, und 5 zurück                       |

> Note: Zunächst war geplant die Berechnung der Distanz mittels Google Maps vorzunehmen. Ansätze dafür sind auch noch vorhanden, jedoch erwies sich dies als sehr Zeitaufwendig.

`year_months.py`:

| Funktion          | Beschreibung                                                                                                |
| ----------------- | ----------------------------------------------------------------------------------------------------------- |
| `get_year_months` | Gibt eine Liste von `yearmonth` Kombinationen zurück basierend auf den Dateien in `/home/airflow/bike_data` |

### Datenbereinigung

Um die Qualität der Daten sicherzustellen, werden die Daten vor der Berechnung der KPIs bereinigt. Hierbei werden die folgenden Schritte ausgeführt:

1. `0 < trip_duration < 86400` (`0` - `24` Stunden)
2. `0 < age < 100`
3. `0 < generation` (`-1` repräsentiert einen einen Fehler)
4. `(0 < timeslot_[0 | 1 | 2 | 3]) & (timeslot_[0 & 1 & 2 & 3] != -1)` (`-1` repräsentiert einen einen Fehler)

### Docker Compose

Das Docker Compose File ist in zwei Teile aufgeteilt. Zum einen wird der `spark_base` Container gestartet, welcher Hadoop und Hive beinhaltet. Zum anderen wird der `airflow` Container gestartet, welcher Airflow beinhaltet.

Um das `spark_base` Image kompatibel mit docker compose zu machen ist es notwendig dies ein wenig zu verändern. Dies passiert durch `airflow.dockerfile` und `startup.sh`. Innerhalb der `airflow.dockerfile` werden außerdem die benötigten Python Pakete installiert. Die dockerfile wird durch die docker compose gebuilded. Anschließend werden die lokalen Ordner `dags` und `data` an den `airflow` Container gemounted.

## Probleme

1. Wenn ich die docker compose lokal ausgeführt habe, beendet der `hiveserver2` sich immer nach `30` Verbindungen. Sobald man den `hiveserver2` einfach erneut startet und den dag ausführt, läuft alles wieder.
2. In der docker compose werden einerseits Ordner wie `dags`, `python` und `output` gemounted. Bei letzterem kam es zu Problemen, da der Ordner nicht von einem lokalen Nutzer im Container erstellt wurde. Mittels dem folgenden Befehl können die notwendigen Berechtigungen angepasst werden.

    ```bash
    sudo chmod 777 data/output/
    ```

3. Wenn die docker compose auf einem Windows Rechner ausgeführt wird, kann es dazu kommen, dass die `EOL` von `LF` sich auf `CRLF` ändert. Dies führt dazu, dass der `airflow` Container nicht starten kann.
