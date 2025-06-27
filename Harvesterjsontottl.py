import http.client
import requests
import json
import time
import os
from datetime import datetime
from rdflib import Graph, Namespace, Literal, URIRef, BNode, RDF

# ==== Configuration ==== #

JSON_DIR = "json_chunks"
TTL_DIR = "ttl_chunks"
OUTPUT_DIR = "output"
OUTPUT_FILE = "output/all_data.ttl"
os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(TTL_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
DELETE_TEMP_TTL = True
Rows = 100 # number of records per chunk
MAX_CHUNKS = 1 # max count of chunks mostly for testing purposes
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

# ==== Namespaces ==== #
CTO = Namespace("https://nfdi4culture.de/ontology#")
N4C = Namespace("https://nfdi4culture.de/id/")
NFDICORE = Namespace("https://nfdi.fiz-karlsruhe.de/ontology#")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SCHEMA = Namespace("http://schema.org/")
XSD = Namespace("http://www.w3.org/2001/XMLSchema#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

# ==== Auxiliary function for creating a TTL datafeed element ==== #
def make_ttl_entry(doc, graph):
    id_ = doc.get("id")
    title = doc.get("paper_title", "").replace('"', "'")
    pub_date = doc.get("publication_date", "1900-01-01T00:00:00Z")[:10]
    lang_code_raw = doc.get("language", ["und"])[0]
    language = LANGUAGE_MAP.get(lang_code_raw, lang_code_raw)
    provider_id = doc.get("provider_ddb_id")
    places = doc.get("place_of_distribution", [])

    item_uri = URIRef(f"https://www.deutsche-digitale-bibliothek.de/newspaper/item/{id_}")
    graph.add((item_uri, RDF.type, CTO.DatafeedElement))
    graph.add((item_uri, RDF.type, CTO.Item))
    graph.add((item_uri, RDFS.label, Literal(title, lang="de")))
    graph.add((item_uri, SCHEMA.url, Literal(f"https://www.deutsche-digitale-bibliothek.de/newspaper/item/{id_}", datatype = SCHEMA.URL)))
    graph.add((item_uri, NFDICORE.license, URIRef("https://creativecommons.org/publicdomain/zero/1.0/")))
    graph.add((item_uri, NFDICORE.publisher, N4C["E1883"]))
    graph.add((item_uri, SCHEMA.sourceOrganization,
               URIRef(f"https://www.deutsche-digitale-bibliothek.de/organization/{provider_id}")))
    graph.add(( URIRef(f"https://www.deutsche-digitale-bibliothek.de/organization/{provider_id}"), RDF.type, NFDICORE.Organization))
    graph.add((item_uri, CTO.creationDate, Literal(pub_date, datatype=XSD.date)))
    graph.add((item_uri, CTO.elementOf, N4C["E6349"]))
    graph.add((item_uri, CTO.elementType, URIRef("http://vocab.getty.edu/page/aat/300026656")))
    #for place in places:
    #    graph.add((item_uri, CTO.relatedLocationLiteral, Literal(place)))
    #graph.add((item_uri, SCHEMA.inLanguage, Literal(language)))

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


consecutive_error_count = 0

while True:
    if idx >= MAX_CHUNKS:
        print("Maximum number of chunks reached.")
        break
    session = requests.session()
    try:
        API_URL = f"https://api.deutsche-digitale-bibliothek.de/search/index/newspaper-issues/select?q=type:issue&rows={Rows}&start={start}"
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
                    graph = Graph()
                    graph.bind("cto", CTO)
                    graph.bind("n4c", N4C)
                    graph.bind("nfdicore", NFDICORE)
                    graph.bind("rdfs", RDFS)
                    graph.bind("schema", SCHEMA, replace=True)
                    graph.bind("xsd", XSD)
                    for doc in docs:
                        make_ttl_entry(doc, graph)
                        all_ids.append(doc.get("id"))
                    ttl_chunk_path = f"{TTL_DIR}/ttl_chunk_{idx}.ttl"
                    graph.serialize(destination=ttl_chunk_path, format="turtle")
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
    print("\nCombining all ttl chunks...")
    combined_graph = Graph()
    combined_graph.bind("cto", CTO)
    combined_graph.bind("n4c", N4C)
    combined_graph.bind("nfdicore", NFDICORE)
    combined_graph.bind("rdfs", RDFS)
    combined_graph.bind("schema", SCHEMA, replace=True)
    combined_graph.bind("xsd", XSD)
    combined_graph.bind("rdf", RDF)
    for filename in os.listdir(TTL_DIR):
        if filename.startswith("ttl_chunk_") and filename.endswith(".ttl"):
            filepath = os.path.join(TTL_DIR, filename)
            combined_graph.parse(filepath, format="turtle")
    today = datetime.now().strftime("%Y-%m-%d")
    datafeed_items = []
    for id_ in all_ids:
        datafeed_item = BNode()
        combined_graph.add((datafeed_item, RDF.type, SCHEMA.DataFeedItem))
        combined_graph.add(
            (datafeed_item, SCHEMA.item, URIRef(f"https://www.deutsche-digitale-bibliothek.de/newspaper/item/{id_}")))
        datafeed_items.append(datafeed_item)
    combined_graph.add((N4C["E6349"], RDF.type, SCHEMA.DataFeed))
    combined_graph.add((N4C["E6349"], SCHEMA.dataFeedElement, datafeed_items[0]))
    for i in range(1, len(datafeed_items)):
        combined_graph.add((N4C["E6349"], SCHEMA.dataFeedElement, datafeed_items[i]))
    combined_graph.add((N4C["E6349"], SCHEMA.dateModified, Literal(today, datatype=XSD.date)))
    combined_graph.serialize(destination=OUTPUT_FILE, format="turtle")
    print(f"Combined: {OUTPUT_FILE}")
    merge_duration = time.time() - merge_start
else:
    merge_duration = 0

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