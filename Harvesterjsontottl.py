
import http.client
import requests
import json
import time
import os
from datetime import datetime

# ==== Configuration ==== #

JSON_DIR = "json_chunks"
TTL_DIR = "ttl_chunks"
OUTPUT_DIR = "output"
OUTPUT_FILE = "output/all_data.ttl"
os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(TTL_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
DELETE_TEMP_TTL = False
Rows = 1000000 # number of records per chunk
MAX_CHUNKS = 5 # max count of chunks mostly for testing purposes
STATE_FILE = "state.json" # state file incase an error occurs
DELETE_STATE = True # deleting state file after successful run
GENERATE_TTL = True
MAX_CONSECUTIVE_ERRORS = 10


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
def save_state(idx, start, all_ids):
    state = {
        "idx": idx,
        "start": start,
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

# ==== Processing: Load JSON → optional save to TTL ==== #
print("Start JSON retrieval and optional TTL generation...")
# Load the state if available
state = load_state()
if state:
    idx = state["idx"]
    start = state["start"]
    all_ids = state["all_ids"]
    print(f"State loaded: Index={idx}, Start={start}")
else:
    idx = 0
    start = 0
    all_ids = []

API_URL = f"https://api.deutsche-digitale-bibliothek.de/search/index/newspaper-issues/select?q=type:issue&rows={Rows}&start={start}"
consecutive_error_count = 0

while True:
    if idx >= MAX_CHUNKS:
        print("Maximum number of chunks reached.")
        break
    session = requests.session()
    try:
        response = session.get(API_URL, timeout=60*10)
        if response.status_code == 200:
            try:
                data = response.json()
                docs = data.get("response", {}).get("docs", [])
                cursor = data.get("nextCursorMark")
                if not docs:
                    print("Call-of completed.")
                    break
                json_chunks_path = f"{JSON_DIR}/data_chunk_{idx}.json"
                with open(json_chunks_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
                print(f"JSON written: {json_chunks_path} ({len(docs)} Records)")
                if GENERATE_TTL:
                    ttl_chunk_path = f"{TTL_DIR}/ttl_chunk_{idx}.ttl"
                    with open(ttl_chunk_path, "w", encoding="utf-8") as f:
                        for doc in docs:
                            ttl_entry = make_ttl_entry(doc)
                            f.write(ttl_entry + "\n\n")
                            all_ids.append(doc.get("id"))
                    print(f"TTL written: {ttl_chunk_path} ({len(docs)} Records)")
                start += len(docs)
                idx += 1
                save_state(idx, start, all_ids)  # saving state
                if response.elapsed.total_seconds() > 1.5:
                    time.sleep(0.2)  # giving api a pause
            except json.JSONDecodeError:
                print(f"Failed to decode JSON from {API_URL}.")
                consecutive_error_count += 1
                if consecutive_error_count >= MAX_CONSECUTIVE_ERRORS:
                    print(f"Max consecutive errors ({MAX_CONSECUTIVE_ERRORS}) reached. Exiting.")
                    break
                else:
                    print(f"Retrying... ({consecutive_error_count}/{MAX_CONSECUTIVE_ERRORS})")
                    continue
        else:
            print(f"Failed to fetch data from {API_URL}. Status Code: {response.status_code}")
            consecutive_error_count += 1
            if consecutive_error_count >= MAX_CONSECUTIVE_ERRORS:
                print(f"Max consecutive errors ({MAX_CONSECUTIVE_ERRORS}) reached. Exiting.")
                break
            else:
                print(f"Retrying... ({consecutive_error_count}/{MAX_CONSECUTIVE_ERRORS})")
                continue
    except http.client.RemoteDisconnected as e:
        print(f"Error: {e}. Trying again from chunk {idx}.")
        consecutive_error_count += 1
        if consecutive_error_count >= MAX_CONSECUTIVE_ERRORS:
            print(f"Max consecutive errors ({MAX_CONSECUTIVE_ERRORS}) reached. Exiting.")
            break
        else:
            print(f"Retrying... ({consecutive_error_count}/{MAX_CONSECUTIVE_ERRORS})")
            continue
    except requests.RequestException as e:
        print(f"Request failed for {API_URL}: {e}")
        consecutive_error_count += 1
        if consecutive_error_count >= MAX_CONSECUTIVE_ERRORS:
            print(f"Max consecutive errors ({MAX_CONSECUTIVE_ERRORS}) reached. Exiting.")
            break
        else:
            print(f"Retrying... ({consecutive_error_count}/{MAX_CONSECUTIVE_ERRORS})")
            continue
    except Exception as e:
        print(f"Unknown Error: {e}. Exiting the script.")
        break

json_and_ttl_duration = time.time() - json_and_ttl_start

# ==== Combining ttl files ==== #
if GENERATE_TTL:
    merge_start = time.time()
    print("\n Combining all ttl chunks...")

    # Build Header
    prefixes = """@prefix cto: <https://nfdi4culture.de/ontology#> .
@prefix n4c: <https://nfdi4culture.de/id/> .
@prefix nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"""

    all_ttl = [prefixes]

    ttl_file_paths = sorted(
        [os.path.join(TTL_DIR, f) for f in os.listdir(TTL_DIR) if f.startswith("ttl_chunk_") and f.endswith(".ttl")])
    for filepath in ttl_file_paths:
        with open(filepath, "r", encoding="utf-8") as f:
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
    merge_duration = time.time() - merge_start

# ==== Deleting ttl chunks (optional) ==== #
if DELETE_TEMP_TTL:
    # Lösche alle TTL-Dateien im TTL_DIR-Verzeichnis
    for filename in os.listdir(TTL_DIR):
        if filename.startswith("ttl_chunk_") and filename.endswith(".ttl"):
            filepath = os.path.join(TTL_DIR, filename)
            try:
                os.remove(filepath)
                print(f"Deleted: {filepath}")
            except Exception as e:
                print(f"An Error occurred while deleting {filepath}: {e}")
else:
    print("\nTemporary files were kept (DELETE_TEMP_FILES = False).")



total_duration = time.time() - overall_start

# ==== Displaying results ==== #
print("\n️Time stats:")
print(f"JSON-Download (+ TTL-Generation): {json_and_ttl_duration:.2f} Sekunden")
if GENERATE_TTL:
    print(f"TTL-Combining:           {merge_duration:.2f} Sekunden")
print(f"Total time:      {total_duration:.2f} Sekunden")

# Removes the state file after successful execution
if DELETE_STATE:
    try:
        os.remove(STATE_FILE)
        print(f"Deleted: {STATE_FILE}")
    except Exception as e:
        print(f"Error when deleting the status file: {e}")
else:
    print("\nState file not deleted.")