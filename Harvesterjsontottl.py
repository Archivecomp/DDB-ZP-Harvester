
import http.client
import requests
import json
import time
import os
from datetime import datetime

# ==== Settings ==== #
API_URL = "https://api.deutsche-digitale-bibliothek.de/search/index/newspaper-issues/select"

TTL_DIR = "ttl_chunks"
OUTPUT_FILE = "output/all_data.ttl"
os.makedirs(TTL_DIR, exist_ok=True)
os.makedirs("output", exist_ok=True)
DELETE_TEMP_TTL = True
Rows = 10000 # number of records per chunk
MAX_CHUNKS = 3 # max count of chunks mostly for testing purposes
STATE_FILE = "state.json" # state file incase an error occurs
QUERY_PARAMS = {
    "q": "type:issue",
    "rows": {Rows},
    "sort": "id ASC",
    "cursorMark": "*",
}

# ==== Mapping ==== #
LANGUAGE_MAP = {
    "ger": "de",
    "eng": "en",
    "fre": "fr",
    "ita": "it",
    "spa": "es"
}

# ==== Auxiliary function for creating a TTL datafeed element ==== #
def make_ttl_entry(doc):
    id_ = doc.get("id")
    title = doc.get("paper_title", "").replace('"', "'")
    pub_date = doc.get("publication_date", "1900-01-01T00:00:00Z")[:10]
    lang_code_raw = doc.get("language", ["und"])[0]
    language = LANGUAGE_MAP.get(lang_code_raw, lang_code_raw)
    provider_id = doc.get("provider_ddb_id")

    # extract places
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
    schema:sourceOrganization "https://www.deutsche-digitale-bibliothek.de/organization/{provider_id}" ;
    cto:creationDate "{pub_date}"^^xsd:date ;
    cto:elementOf n4c:E6349 ;
    cto:elementType <http://vocab.getty.edu/page/aat/300026656> ;
    {place_str}
    schema:inLanguage "{language}" .
""".strip()

# ==== Auxiliary function for saving the status ==== #
def save_state(chunk_index, cursor, all_ids):
    state = {
        "chunk_index": chunk_index,
        "cursor": cursor,
        "all_ids": all_ids
    }
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)

# ==== Auxiliary function for loading the status ==== #
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

# ==== Timekeeping ==== #
overall_start = time.time()
json_and_ttl_start = time.time()

# ==== Processing: Load JSON → save directly to TTL ==== #
print("Start JSON retrieval and direct TTL generation...")
# Load the state if available
state = load_state()
if state:
    chunk_index = state["chunk_index"]
    params = QUERY_PARAMS.copy()
    params["cursorMark"] = state["cursor"]
    all_ids = state["all_ids"]
    print(f"State loaded: Chunk Index={chunk_index}, Cursor={params['cursorMark']}")
else:
    chunk_index = 0
    params = QUERY_PARAMS.copy()
    all_ids = []

seen_cursors = set()
ttl_chunk_paths = []

while True:
    if chunk_index >= MAX_CHUNKS:
        print("Maximum number of chunks reached.")
        break
    session = requests.session()
    try:
        response = session.get(API_URL, params=params)
        if response.status_code == 200:
            try:
                data = response.json()
                docs = data.get("response", {}).get("docs", [])
                cursor = data.get("nextCursorMark")
                if not docs or cursor in seen_cursors:
                    print("Call-of completed.")
                    break
                # TTL-Datei für diesen Chunk direkt schreiben
                ttl_chunk_path = f"{TTL_DIR}/ttl_chunk_{chunk_index}.ttl"
                with open(ttl_chunk_path, "w", encoding="utf-8") as f:
                    for doc in docs:
                        ttl_entry = make_ttl_entry(doc)
                        f.write(ttl_entry + "\n\n")
                        all_ids.append(doc.get("id"))
                print(f"TTL written: {ttl_chunk_path} ({len(docs)} Records)")
                ttl_chunk_paths.append(ttl_chunk_path)
                seen_cursors.add(cursor)
                params["cursorMark"] = cursor
                chunk_index += 1
                save_state(chunk_index, cursor, all_ids)  # saving state
                if response.elapsed.total_seconds() > 1.5:
                    time.sleep(0.2)  # giving api a pause
            except json.JSONDecodeError:
                print(f"Failed to decode JSON from {API_URL}.")
                break
        else:
            print(f"Failed to fetch data from {API_URL}. Status Code: {response.status_code}")
            break
    except http.client.RemoteDisconnected as e:
        print(f"Error: {e}. Trying again from chunk {chunk_index}.")
        continue
    except requests.RequestException as e:
        print(f"Request failed for {API_URL}: {e}")
        continue
    except Exception as e:
        print(f"Unknown Error: {e}. Exiting the script.")
        break

json_and_ttl_duration = time.time() - json_and_ttl_start

# ==== Combining ttl files ==== #
merge_start = time.time()
print("\n Combining all ttl chunks...")

prefixes = """@prefix cto: <https://nfdi4culture.de/ontology#> .
@prefix n4c: <https://nfdi4culture.de/id/> .
@prefix nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"""

all_ttl = [prefixes]

for path in ttl_chunk_paths:
    with open(path, "r", encoding="utf-8") as f:
        all_ttl.append(f.read())

# Build closing block
today = datetime.now().strftime("%Y-%m-%d")
datafeed_items = ",\n        ".join([
    f"""[ a schema:DataFeedItem ;
            schema:item <https://www.deutsche-digitale-bibliothek.de/newspaper/item/{id_}> ]"""
    for id_ in all_ids
])
footer = f"""
n4c:E6349 a schema:DataFeed ;
    schema:dataFeedElement {datafeed_items} ;
    schema:dataModified "{today}"^^xsd:date .
""".strip()

all_ttl.append(footer)

with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
    out.write("\n\n".join(all_ttl))
print(f"Combined: {OUTPUT_FILE}")

# ==== Deleting ttl chunks (optional) ==== #
if DELETE_TEMP_TTL:
    for path in ttl_chunk_paths:
        try:
            os.remove(path)
            print(f"Deleted: {path}")
        except Exception as e:
            print(f"An Error occurred while deleting {path}: {e}")
else:
    print("\nTemporary files were kept (DELETE_TEMP_FILES = False).")

merge_duration = time.time() - merge_start
total_duration = time.time() - overall_start

# ==== Displaying results ==== #
print("\n️Time stats:")
print(f"JSON-Download + TTL-Generation: {json_and_ttl_duration:.2f} Sekunden")
print(f"TTL-Combining:           {merge_duration:.2f} Sekunden")
print(f"Total time:      {total_duration:.2f} Sekunden")

# Removes the state file after successful execution
try:
    os.remove(STATE_FILE)
except Exception as e:
    print(f"Error when deleting the status file: {e}")