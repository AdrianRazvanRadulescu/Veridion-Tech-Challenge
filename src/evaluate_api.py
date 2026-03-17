import time
import pandas as pd
import json
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

API_ENDPOINT_URL = "http://127.0.0.1:8000/match"
CONCURRENT_WORKERS = 10


def extract_column_value(dataframe_row, possible_columns):
    for col in possible_columns:
        if col in dataframe_row.index and not pd.isna(dataframe_row[col]):
            val = str(dataframe_row[col]).strip()
            if val.lower() != "nan" and val != "":
                return val
    return ""


def test_single_api_endpoint(dataframe_row):
    payload = {
        "name": extract_column_value(
            dataframe_row, ["name", "company_name", "company", "legal_name"]
        ),
        "website": extract_column_value(
            dataframe_row, ["website", "domain", "url", "site", "web"]
        ),
        "phone": extract_column_value(
            dataframe_row, ["phone", "phone_number", "telephone", "contact"]
        ),
        "facebook_profile": extract_column_value(
            dataframe_row, ["facebook_profile", "facebook", "social", "social_media"]
        ),
    }

    query_string = urllib.parse.urlencode(payload)
    url = f"{API_ENDPOINT_URL}?{query_string}"

    try:
        req = urllib.request.Request(url, method="POST")
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.getcode() == 200:
                data = json.loads(response.read().decode("utf-8"))
                return True, data.get("match_confidence_score", 0)
    except Exception:
        pass
    return False, 0


def run_evaluation_suite():
    try:
        evaluation_dataframe = pd.read_csv("data/API-input-sample.csv")
    except FileNotFoundError:
        print(
            "Test CSV not found. Please ensure 'API-input-sample.csv' is in the 'data' folder."
        )
        return

    total_records = len(evaluation_dataframe)

    print(f"\nInitiating Threaded API Evaluation on {total_records} records...")
    start_time = time.time()

    successful_matches = 0
    total_confidence = 0

    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        futures = [
            executor.submit(test_single_api_endpoint, row)
            for _, row in evaluation_dataframe.iterrows()
        ]

        for future in tqdm(
            as_completed(futures),
            total=total_records,
            desc="Evaluating API",
            unit="req",
        ):
            success, score = future.result()
            if success:
                successful_matches += 1
                total_confidence += score

    end_time = time.time()
    duration = end_time - start_time
    failed_matches = total_records - successful_matches

    match_rate = (successful_matches / total_records) * 100 if total_records > 0 else 0
    avg_score = (total_confidence / successful_matches) if successful_matches > 0 else 0

    print("\n" + "=" * 60)
    print("API MATCHING ENGINE EVALUATION REPORT")
    print("=" * 60)
    print(f"Total Records Tested   : {total_records}")
    print(f"Total Execution Time   : {duration:.2f} seconds")
    print(f"Throughput             : {total_records / duration:.2f} req/sec")
    print(f"Successful Matches     : {successful_matches}")
    print(f"Failed/Not Found       : {failed_matches}")
    print(f"Avg Confidence Score   : {avg_score:.2f}")
    print("-" * 60)
    print(f"FINAL MATCH RATE       : {match_rate:.2f}%")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run_evaluation_suite()
