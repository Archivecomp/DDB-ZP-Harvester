import requests
import json
import time
import os
from datetime import datetime

# ==== Einstellungen ==== #
API_URL = "https://api.deutsche-digitale-bibliothek.de/search/index/newspaper-issues/select"
QUERY_PARAMS = {
    "q": "type:issue",
    "rows": 1000,
    "sort": "id ASC",
    "cursorMark": "*",
}

TTL_DIR = "ttl_chunks"
OUTPUT_FILE = "output/all_data.ttl"
os.makedirs(TTL_DIR, exist_ok=True)
os.makedirs("output", exist_ok=True)
DELETE_TEMP_TTL = True
MAX_CHUNKS = 20  # maximale Anzahl JSON-Chunks für Testzwecke festlegen

# ==== Mapping ==== #
LANGUAGE_MAP = {
    "ger": "de",
    "eng": "en",
    "fre": "fr",
    "ita": "it",
    "spa": "es"
}

PROVIDER_MAP = {
    "VKNQFFAKOR4XZWJJKUX3NGYSZ3QZAXCW": "n4c:E2295",
    "NEOOVZ6BJZD3LYNKU3N7PFT6VD54SMMG": "",
    "VNHXUCEEKHOUSYH4NVOUBHJGSRMOGK7J": "",
    "6GFV3I4ELFEEFQIN2WECOXMTI5FUWHCK": "n4c:E2031",
    "INLVDM4I3AMZLTG6AE6C5GZRJKGOF75K": "",
    "BZVTR553HLJBDMQD5NCJ6YKP3HMBQRF4": "",
    "265BI7NE7QBS4NQMZCCGIVLFR73OCOSL": "n4c:E1841",
    "Q5Q6S6XOPTGP3BUM4I2JNP7V53BAWOTT": "n4c:E1980",
    "4EV676FQPACNVNHFEJHGKUY55BXC3QMB": "",
    "7M6B7VD3Y42GLVM75OGK3VNK62VU4OGF": "",
    "A5MCTVQDBFJTFJDZRTIZX5W6YKSHWOVC": "",
    "CZTZO4SBNHW34JVKYRWW67725WWGLZA5": "",
    "RMHO6ZMQPXRNLKEUNW3VG2B563ALDO5S": "",
    "NWNEPSPSGSSYWU3IP75BYGGBRNQORN6A": "",
    "4VUN5X2CVCEDV63QYFG6ZSJ4NUNTQWHB": "",
    "56EXT7QNQMRDQY4ZZWQGOLXGR3IN3C4F": "",
    "ZVFWDOJAROVAMTFCAFPSEAXTMRBD35RE": "",
    "VXSBME756Q77YO6NJWC5QFZ6D4VLVWG3": "",
    "3HK6MSZN45JDHFPFYSN2Z476QKJPJSRA": "n4c:E1979",
    "UPHR66ECKLOQBHTW23IVD2SE4UBEF2XY": "",
    "UJVQP2TIF4YVCCJQXWZ2BNEECO7ZHYTW": "",
    "ZS4SLW4XQJ6WDOGVMGMCOBHICZV7CKBX": ""
}
# ==== Hilfsfunktion zum Erzeugen eines TTL-DatafeedElements ==== #
def make_ttl_entry(doc):
    id_ = doc.get("id")
    title = doc.get("paper_title", "").replace('"', "'")
    pub_date = doc.get("publication_date", "1900-01-01T00:00:00Z")[:10]
    lang_code_raw = doc.get("language", ["und"])[0]
    language = LANGUAGE_MAP.get(lang_code_raw, lang_code_raw)
    provider_id_raw = doc.get("provider_ddb_id")
    provider_id = PROVIDER_MAP.get(provider_id_raw, provider_id_raw)

    # Ort(e) extrahieren
    places = doc.get("place_of_distribution", [])
    place_str = ""
    if places:
        place_literals = ',\n        '.join(f'"{place}"' for place in places)
        place_str = f'cto:relatedLocationLiteral {place_literals} ;'

    return f"""
<https://www.deutsche-digitale-bibliothek.de/newspaper/item/{id_}> a cto:DatafeedElement,
        cto:Item ;
    rdfs:label "{title}"@de ;
    schema:url <https://www.deutsche-digitale-bibliothek.de/newspaper/item/{id_}> ;
    nfdicore:license <https://creativecommons.org/publicdomain/zero/1.0/> ;
    nfdicore:publisher n4c:E1883 ;
    schema:provider {provider_id} ;
    cto:creationDate "{pub_date}"^^xsd:date ;
    cto:elementOf n4c:E6349 ;
    cto:elementType <http://vocab.getty.edu/page/aat/300026656> ;
    {place_str}
    schema:inLanguage "{language}" ;
    ?property <https://nfdi4culture.de/id/E1883> .
""".strip()

# ==== Zeitmessung ==== #
overall_start = time.time()
json_and_ttl_start = time.time()

# ==== Verarbeitung: JSON laden → direkt in TTL speichern ==== #
print("Starte JSON-Abruf und direkte TTL-Generierung...")
seen_cursors = set()
chunk_index = 0
params = QUERY_PARAMS.copy()
ttl_chunk_paths = []
all_ids = []

while True:
    if chunk_index >= MAX_CHUNKS:
        print("Maximale Anzahl von Chunks erreicht.")
        break

    session = requests.session()
    response = session.get(API_URL, params=params)
    data = response.json()

    docs = data.get("response", {}).get("docs", [])
    cursor = data.get("nextCursorMark")

    if not docs or cursor in seen_cursors:
        print("Abruf abgeschlossen.")
        break

    # TTL-Datei für diesen Chunk direkt schreiben
    ttl_chunk_path = f"{TTL_DIR}/ttl_chunk_{chunk_index}.ttl"
    with open(ttl_chunk_path, "w", encoding="utf-8") as f:
        for doc in docs:
            ttl_entry = make_ttl_entry(doc)
            f.write(ttl_entry + "\n\n")
            all_ids.append(doc.get("id"))

    print(f"TTL geschrieben: {ttl_chunk_path} ({len(docs)} Einträge)")
    ttl_chunk_paths.append(ttl_chunk_path)

    seen_cursors.add(cursor)
    params["cursorMark"] = cursor
    chunk_index += 1
    if response.elapsed.total_seconds() > 1.5:
        time.sleep(0.2)  # Rücksicht auf API

json_and_ttl_duration = time.time() - json_and_ttl_start

# ==== TTL-Dateien zusammenführen ==== #
merge_start = time.time()
print("\n Füge alle TTL-Chunks zusammen...")

prefixes = """@prefix cto: <https://nfdi4culture.de/ontology#> .
@prefix n4c: <https://nfdi4culture.de/id/> .
@prefix nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n"""

all_ttl = [prefixes]

for path in ttl_chunk_paths:
    with open(path, "r", encoding="utf-8") as f:
        all_ttl.append(f.read())

# Abschlussblock bauen
today = datetime.now().strftime("%Y-%m-%d")
datafeed_items = ",\n        ".join([
    f"""[ a schema:DataFeedItem ;
            schema:item <https://www.deutsche-digitale-bibliothek.de/newspaper/item/{id_}> ]"""
    for id_ in all_ids
])
footer = f"""
n4c:Exxxx a schema:DataFeed ;
    schema:dataFeedElement {datafeed_items} ;
    schema:dataModified "{today}"^^xsd:date .
""".strip()

all_ttl.append(footer)

with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
    out.write("\n\n".join(all_ttl))
print(f"Zusammengeführt: {OUTPUT_FILE}")

# ==== TTL-Chunks löschen (optional) ==== #
if DELETE_TEMP_TTL:
    for path in ttl_chunk_paths:
        try:
            os.remove(path)
            print(f"Gelöscht: {path}")
        except Exception as e:
            print(f"Fehler beim Löschen von {path}: {e}")
else:
    print("\nTemporäre Dateien wurden behalten (DELETE_TEMP_FILES = False).")

merge_duration = time.time() - merge_start
total_duration = time.time() - overall_start

# ==== Ergebnis anzeigen ==== #
print("\n️Zeitstatistik:")
print(f"JSON-Download + TTL-Erzeugung: {json_and_ttl_duration:.2f} Sekunden")
print(f"TTL-Zusammenführung:           {merge_duration:.2f} Sekunden")
print(f"Gesamtverarbeitungsdauer:      {total_duration:.2f} Sekunden")

