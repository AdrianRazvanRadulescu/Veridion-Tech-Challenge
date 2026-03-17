import os
from fastapi import FastAPI, HTTPException
from elasticsearch import AsyncElasticsearch

application_api = FastAPI(title="Company Identity Resolution API")
ELASTICSEARCH_CONNECTION_URL = os.environ.get(
    "ELASTICSEARCH_URL", "http://elasticsearch:9200"
)
async_elasticsearch_client = AsyncElasticsearch(ELASTICSEARCH_CONNECTION_URL)
ELASTICSEARCH_INDEX_NAME = "company_profiles"


def construct_elasticsearch_bool_query(
    target_name, target_website, target_phone, target_facebook
):
    should_match_conditions = []

    if target_website:
        clean_web = (
            target_website.replace("http://", "")
            .replace("https://", "")
            .replace("www.", "")
            .strip("/")
            .lower()
        )
        should_match_conditions.append(
            {"wildcard": {"domain": {"value": f"*{clean_web}*", "boost": 10.0}}}
        )

    if target_phone:
        should_match_conditions.append(
            {"match": {"extracted_phones": {"query": target_phone, "boost": 5.0}}}
        )

    if target_name:
        should_match_conditions.append(
            {"match": {"company_name.ngram": {"query": target_name, "boost": 2.0}}}
        )
        should_match_conditions.append(
            {
                "match": {
                    "company_name": {
                        "query": target_name,
                        "fuzziness": "AUTO",
                        "boost": 1.5,
                    }
                }
            }
        )

    if target_facebook:
        clean_fb = (
            target_facebook.replace("http://", "")
            .replace("https://", "")
            .replace("www.", "")
            .strip("/")
            .lower()
        )
        should_match_conditions.append(
            {
                "wildcard": {
                    "extracted_social_links": {"value": f"*{clean_fb}*", "boost": 3.0}
                }
            }
        )

    constructed_query = {
        "bool": {"should": should_match_conditions, "minimum_should_match": 1}
    }
    return constructed_query


@application_api.post("/match")
async def resolve_company_identity(
    name: str = "", website: str = "", phone: str = "", facebook_profile: str = ""
):
    if not name and not website and not phone and not facebook_profile:
        raise HTTPException(
            status_code=400, detail="At least one search parameter must be provided."
        )

    es_query = construct_elasticsearch_bool_query(
        name, website, phone, facebook_profile
    )

    try:
        search_response = await async_elasticsearch_client.search(
            index=ELASTICSEARCH_INDEX_NAME, query=es_query, size=1
        )

        search_hits = search_response["hits"]["hits"]

        if not search_hits:
            raise HTTPException(status_code=404, detail="No match found")

        best_match_result = search_hits[0]

        response_payload = {
            "match_confidence_score": best_match_result["_score"],
            "resolved_company_profile": best_match_result["_source"],
        }

        return response_payload
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
