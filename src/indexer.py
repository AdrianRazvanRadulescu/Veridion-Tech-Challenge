import os
import pandas as pd
import json
from elasticsearch import Elasticsearch, helpers

ELASTICSEARCH_INDEX_NAME = "company_profiles"
ELASTICSEARCH_CONNECTION_URL = os.environ.get(
    "ELASTICSEARCH_URL", "http://elasticsearch:9200"
)

INDEX_SETTINGS = {
    "index": {"max_ngram_diff": 10},
    "analysis": {
        "analyzer": {
            "company_name_ngram_analyzer": {
                "tokenizer": "custom_ngram_tokenizer",
                "filter": ["lowercase", "asciifolding"],
            },
            "clean_phone_analyzer": {
                "tokenizer": "keyword",
                "char_filter": ["numeric_only_char_filter"],
            },
        },
        "tokenizer": {
            "custom_ngram_tokenizer": {
                "type": "ngram",
                "min_gram": 3,
                "max_gram": 10,
                "token_chars": ["letter", "digit"],
            }
        },
        "char_filter": {
            "numeric_only_char_filter": {
                "type": "pattern_replace",
                "pattern": "[^0-9]",
                "replacement": "",
            }
        },
    },
}

INDEX_MAPPINGS = {
    "properties": {
        "company_name": {
            "type": "text",
            "fields": {
                "ngram": {
                    "type": "text",
                    "analyzer": "company_name_ngram_analyzer",
                },
                "keyword": {"type": "keyword"},
            },
        },
        "domain": {"type": "keyword"},
        "extracted_phones": {"type": "text", "analyzer": "clean_phone_analyzer"},
        "extracted_social_links": {"type": "keyword"},
        "extracted_addresses": {"type": "text"},
    }
}


def execute_data_merge_and_indexing():
    company_names_dataframe = pd.read_csv("data/sample-websites-company-names.csv")

    with open("data/scraped_data.json", "r", encoding="utf-8") as json_file:
        scraped_data_list = json.load(json_file)

    scraped_dataframe = pd.DataFrame(scraped_data_list)

    merged_final_dataframe = pd.merge(
        company_names_dataframe, scraped_dataframe, on="domain", how="left"
    )

    merged_final_dataframe["extracted_phones"] = merged_final_dataframe[
        "extracted_phones"
    ].apply(lambda items: items if isinstance(items, list) else [])
    merged_final_dataframe["extracted_social_links"] = merged_final_dataframe[
        "extracted_social_links"
    ].apply(lambda items: items if isinstance(items, list) else [])
    merged_final_dataframe["extracted_addresses"] = merged_final_dataframe[
        "extracted_addresses"
    ].apply(lambda items: items if isinstance(items, list) else [])
    merged_final_dataframe = merged_final_dataframe.fillna("")

    elasticsearch_client = Elasticsearch(
        ELASTICSEARCH_CONNECTION_URL, request_timeout=30
    )

    elasticsearch_client.options(ignore_status=[400, 404]).indices.delete(
        index=ELASTICSEARCH_INDEX_NAME
    )

    elasticsearch_client.indices.create(
        index=ELASTICSEARCH_INDEX_NAME, settings=INDEX_SETTINGS, mappings=INDEX_MAPPINGS
    )

    bulk_indexing_actions = []
    for index, dataframe_row in merged_final_dataframe.iterrows():
        document_payload = {
            "company_name": str(dataframe_row.get("company_name", "")),
            "domain": str(dataframe_row.get("domain", "")),
            "extracted_phones": dataframe_row.get("extracted_phones", []),
            "extracted_social_links": dataframe_row.get("extracted_social_links", []),
            "extracted_addresses": dataframe_row.get("extracted_addresses", []),
        }

        bulk_action_definition = {
            "_index": ELASTICSEARCH_INDEX_NAME,
            "_source": document_payload,
        }
        bulk_indexing_actions.append(bulk_action_definition)

    helpers.bulk(elasticsearch_client, bulk_indexing_actions)
    print(
        f"Successfully merged and indexed {len(bulk_indexing_actions)} corporate records into Elasticsearch."
    )


if __name__ == "__main__":
    execute_data_merge_and_indexing()
