# README

## Overview

This script retrieves data from the Deutsche Digitale Bibliothek API and converts it into TTL files. It supports processing large datasets by using chunks and saves the state between executions to resume in case of errors.

## Configuration

The configuration parameters can be adjusted at the beginning of the script:

* **`API_URL`**: The URL of the API endpoint.
* **`JSON_DIR`**: The directory where raw JSON response files will be stored.
* **`TTL_DIR`**: The directory where temporary TTL files will be stored.
* **`OUTPUT_DIR`**: The directory where the output file with the combined TTL data will be stored.
* **`OUTPUT_FILE`**: The name and path of the output file.
* **`DELETE_TEMP_TTL`**: Specifies whether the temporary TTL files should be deleted after processing.
* **`Rows`**: The number of records per chunk.
* **`MAX_CHUNKS`**: The maximum number of chunks to process (for testing purposes).
* **`STATE_FILE`**: The file where the state between executions will be saved in case of errors.
* **`DELETE_STATE`**: Specifies whether the state file should be deleted after successful execution.
* **`GENERATE_TTL`**: Enables or disables the generation of TTL files.
* **`MAX_CONSECUTIVE_ERRORS`**: The maximum number of consecutive errors allowed before exiting the script.
* **`QUERY_PARAMS`**: The parameters for the API request.

## Workflow

1. **Initialization**:
	* Creates the necessary directories (`json_chunks`, `ttl_chunks`, and `output`).
	* Loads the saved state, if available.
2. **Data Retrieval and TTL Generation**:
	* Retrieves data from the API in chunks.
	* Checks the HTTP status code and decodes the JSON response.
	* Saves the raw JSON response to files in `JSON_DIR`.
	* Generates TTL files for each chunk, if `GENERATE_TTL` is enabled.
	* Saves the state after each successful chunk processing.
3. **Combining TTL Files**:
	* Combines all TTL files into a single output file (`all_data.ttl`), if `GENERATE_TTL` is enabled.
	* Adds prefixes and a closing block to the combined TTL file.
4. **Cleanup**:
	* Deletes the temporary TTL files, if `DELETE_TEMP_TTL` is set to `True`.
	* Deletes the state file after successful execution, if `DELETE_STATE` is set to `True`.

## Structure of Datafeed Element

The structure of the Datafeed Element is defined within the `make_ttl_entry` function, which converts each document into a TTL format.

## Error Handling

The script handles various types of errors:

* **HTTP Status Code**: Checks if the request was successful (Status Code 200).
* **JSON Decoding**: Checks if the JSON response was successfully decoded.
* **Network Errors**: Handles network errors such as `RemoteDisconnected`.
* **Consecutive Errors**: Exits the script after a specified number of consecutive errors (`MAX_CONSECUTIVE_ERRORS`).
```