import asyncio
import re
import time
import json
import warnings
from urllib.parse import urljoin
import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import phonenumbers
from tqdm.asyncio import tqdm
from curl_cffi.requests import AsyncSession

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

LIVE_CONCURRENCY_LIMIT = 80
ARCHIVE_CONCURRENCY_LIMIT = 15
REQUEST_TIMEOUT_SECONDS = 12
WAYBACK_TIMEOUT_SECONDS = 15
MAXIMUM_DEEP_CRAWL_PAGES = 3

TARGET_SOCIAL_DOMAINS = [
    "facebook.com",
    "linkedin.com",
    "twitter.com",
    "instagram.com",
    "x.com",
]

TARGET_DEEP_CRAWL_KEYWORDS = [
    "contact",
    "about",
    "location",
    "legal",
    "terms",
    "privacy",
]

ADDRESS_REGEX_PATTERN = r"\b\d{1,5}\s+[a-zA-Z.\s-]+\s+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Way|Square|Sq|Parkway|Pkwy|Circle|Cir|Highway|Hwy|Place|Pl|Trail|Trl)\b"


def normalize_phone_number_string(raw_phone_string):
    try:
        parsed_phone_object = phonenumbers.parse(str(raw_phone_string), "US")
        if phonenumbers.is_valid_number(parsed_phone_object):
            return phonenumbers.format_number(
                parsed_phone_object, phonenumbers.PhoneNumberFormat.E164
            )
    except phonenumbers.NumberParseException:
        pass
    return None


def score_and_rank_url(url_string):
    url_lower = url_string.lower()
    if "contact" in url_lower:
        return 1
    if "about" in url_lower:
        return 2
    if "location" in url_lower:
        return 3
    if "legal" in url_lower:
        return 4
    if "terms" in url_lower:
        return 5
    if "privacy" in url_lower:
        return 6
    return 99


def extract_structured_and_unstructured_data(html_content, base_url):
    parsed_html_soup = BeautifulSoup(html_content, "html.parser")
    unique_phone_numbers = set()
    unique_social_links = set()
    unique_addresses = set()
    potential_deep_crawl_pages = set()

    for script_tag in parsed_html_soup.find_all("script", type="application/ld+json"):
        try:
            json_data = json.loads(script_tag.string)
            data_items = json_data if isinstance(json_data, list) else [json_data]
            for item in data_items:
                if isinstance(item, dict):
                    if "telephone" in item:
                        normalized_phone = normalize_phone_number_string(
                            item["telephone"]
                        )
                        if normalized_phone:
                            unique_phone_numbers.add(normalized_phone)
                    if "sameAs" in item:
                        same_as_attribute = item["sameAs"]
                        social_links_list = (
                            same_as_attribute
                            if isinstance(same_as_attribute, list)
                            else [same_as_attribute]
                        )
                        for social_link in social_links_list:
                            if any(
                                social_domain in str(social_link).lower()
                                for social_domain in TARGET_SOCIAL_DOMAINS
                            ):
                                unique_social_links.add(str(social_link))
                    if "address" in item:
                        address_attribute = item["address"]
                        if isinstance(address_attribute, dict):
                            address_components = [
                                str(value)
                                for key, value in address_attribute.items()
                                if "address" in key.lower()
                                or "region" in key.lower()
                                or "postal" in key.lower()
                            ]
                            if address_components:
                                unique_addresses.add(", ".join(address_components))
                        elif isinstance(address_attribute, str):
                            unique_addresses.add(address_attribute)
        except Exception:
            pass

    for address_tag in parsed_html_soup.find_all("address"):
        text_address = address_tag.get_text(separator=" ", strip=True)
        if len(text_address) > 5:
            unique_addresses.add(text_address)

    all_anchor_tags = parsed_html_soup.find_all("a", href=True)
    for anchor_tag in all_anchor_tags:
        hyperlink_reference = anchor_tag.get("href", "").strip()
        hyperlink_reference_lower = hyperlink_reference.lower()

        if hyperlink_reference_lower.startswith("tel:"):
            clean_phone_string = hyperlink_reference_lower.replace("tel:", "")
            normalized_phone = normalize_phone_number_string(clean_phone_string)
            if normalized_phone:
                unique_phone_numbers.add(normalized_phone)

        elif any(
            social_domain in hyperlink_reference_lower
            for social_domain in TARGET_SOCIAL_DOMAINS
        ):
            unique_social_links.add(hyperlink_reference)

        elif (
            "maps.google" in hyperlink_reference_lower
            or "goo.gl/maps" in hyperlink_reference_lower
        ):
            unique_addresses.add(hyperlink_reference)

        elif any(
            keyword in hyperlink_reference_lower
            for keyword in TARGET_DEEP_CRAWL_KEYWORDS
        ):
            absolute_contact_url = urljoin(base_url, hyperlink_reference)
            if absolute_contact_url.startswith("http"):
                potential_deep_crawl_pages.add(absolute_contact_url)

    raw_page_text = parsed_html_soup.get_text(separator=" ", strip=True)

    for phone_match in phonenumbers.PhoneNumberMatcher(raw_page_text, "US"):
        formatted_number = phonenumbers.format_number(
            phone_match.number, phonenumbers.PhoneNumberFormat.E164
        )
        unique_phone_numbers.add(formatted_number)

    regex_found_addresses = re.findall(
        ADDRESS_REGEX_PATTERN, raw_page_text, re.IGNORECASE
    )
    for regex_address in regex_found_addresses:
        unique_addresses.add(regex_address)

    return (
        unique_phone_numbers,
        unique_social_links,
        unique_addresses,
        potential_deep_crawl_pages,
    )


async def fetch_historical_data_from_archive(
    client_session, target_domain, archive_semaphore
):
    cdx_api_url = f"https://web.archive.org/cdx/search/cdx?url={target_domain}&matchType=domain&filter=statuscode:200&fl=timestamp,original&collapse=digest&limit=-1"

    async with archive_semaphore:
        try:
            cdx_response = await client_session.get(
                cdx_api_url, timeout=WAYBACK_TIMEOUT_SECONDS
            )
            if cdx_response.status_code == 200 and cdx_response.text:
                cdx_response_lines = cdx_response.text.strip().split("\n")
                if cdx_response_lines:
                    latest_snapshot_data = cdx_response_lines[-1].split()
                    if len(latest_snapshot_data) >= 2:
                        snapshot_timestamp = latest_snapshot_data[0]
                        snapshot_original_url = latest_snapshot_data[1]
                        archive_raw_url = f"https://web.archive.org/web/{snapshot_timestamp}id_/{snapshot_original_url}"

                        archive_response = await client_session.get(
                            archive_raw_url, timeout=WAYBACK_TIMEOUT_SECONDS
                        )
                        if archive_response.status_code == 200:
                            (
                                extracted_phones,
                                extracted_socials,
                                extracted_addresses,
                                _,
                            ) = extract_structured_and_unstructured_data(
                                archive_response.text, archive_raw_url
                            )
                            return (
                                list(extracted_phones),
                                list(extracted_socials),
                                list(extracted_addresses),
                                "Success_Archive",
                            )
        except Exception:
            pass

    return [], [], [], "Failed_Archive"


async def fetch_secondary_page_data(client_session, target_url):
    try:
        http_response = await client_session.get(
            target_url, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True
        )
        if http_response.status_code == 200:
            return extract_structured_and_unstructured_data(
                http_response.text, target_url
            )
    except Exception:
        pass
    return set(), set(), set(), set()


async def process_single_domain_pipeline(
    client_session, target_domain, live_semaphore, archive_semaphore
):
    domain_extraction_result = {
        "domain": target_domain,
        "extracted_phones": [],
        "extracted_social_links": [],
        "extracted_addresses": [],
        "crawl_status": "Failed",
    }

    protocols_to_attempt = [f"https://{target_domain}", f"http://{target_domain}"]
    live_fetch_successful = False

    async with live_semaphore:
        for protocol_url in protocols_to_attempt:
            try:
                http_response = await client_session.get(
                    protocol_url, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True
                )

                if http_response.status_code == 200:
                    page_html_content = http_response.text
                    (
                        extracted_phones,
                        extracted_socials,
                        extracted_addresses,
                        potential_deep_pages,
                    ) = extract_structured_and_unstructured_data(
                        page_html_content, protocol_url
                    )

                    if (
                        not extracted_phones
                        or not extracted_socials
                        or not extracted_addresses
                    ) and potential_deep_pages:
                        sorted_deep_pages = sorted(
                            list(potential_deep_pages), key=score_and_rank_url
                        )[:MAXIMUM_DEEP_CRAWL_PAGES]

                        deep_crawl_tasks = [
                            fetch_secondary_page_data(client_session, subpage_url)
                            for subpage_url in sorted_deep_pages
                        ]
                        deep_crawl_results = await asyncio.gather(*deep_crawl_tasks)

                        for (
                            deep_phones,
                            deep_socials,
                            deep_addresses,
                            _,
                        ) in deep_crawl_results:
                            extracted_phones.update(deep_phones)
                            extracted_socials.update(deep_socials)
                            extracted_addresses.update(deep_addresses)

                    domain_extraction_result["extracted_phones"] = list(
                        extracted_phones
                    )
                    domain_extraction_result["extracted_social_links"] = list(
                        extracted_socials
                    )
                    domain_extraction_result["extracted_addresses"] = list(
                        extracted_addresses
                    )
                    domain_extraction_result["crawl_status"] = "Success_Live"
                    live_fetch_successful = True
                    break
                else:
                    domain_extraction_result["crawl_status"] = (
                        f"HTTP_Error_{http_response.status_code}"
                    )

            except Exception as execution_error:
                domain_extraction_result["crawl_status"] = type(
                    execution_error
                ).__name__

    if (
        not live_fetch_successful
        or "Error" in domain_extraction_result["crawl_status"]
        or domain_extraction_result["crawl_status"] == "Failed"
    ):
        archive_phones, archive_socials, archive_addresses, archive_status = (
            await fetch_historical_data_from_archive(
                client_session, target_domain, archive_semaphore
            )
        )

        if archive_status == "Success_Archive":
            domain_extraction_result["extracted_phones"] = archive_phones
            domain_extraction_result["extracted_social_links"] = archive_socials
            domain_extraction_result["extracted_addresses"] = archive_addresses
            domain_extraction_result["crawl_status"] = "Success_Archive"

    return domain_extraction_result


async def orchestrate_asynchronous_extraction(list_of_domains):
    live_concurrency_semaphore = asyncio.Semaphore(LIVE_CONCURRENCY_LIMIT)
    archive_concurrency_semaphore = asyncio.Semaphore(ARCHIVE_CONCURRENCY_LIMIT)

    async with AsyncSession(impersonate="chrome110") as client_session:
        asynchronous_tasks = [
            process_single_domain_pipeline(
                client_session,
                single_domain,
                live_concurrency_semaphore,
                archive_concurrency_semaphore,
            )
            for single_domain in list_of_domains
        ]
        gathered_results = await tqdm.gather(
            *asynchronous_tasks, desc="Processing Domains", unit="domain"
        )
        return gathered_results


def print_corporate_audit_report(results_dataframe, total_execution_time):
    total_processed_domains = len(results_dataframe)

    live_domains_dataframe = results_dataframe[
        results_dataframe["crawl_status"] == "Success_Live"
    ]
    archive_domains_dataframe = results_dataframe[
        results_dataframe["crawl_status"] == "Success_Archive"
    ]

    total_live_domains = len(live_domains_dataframe)
    total_archive_domains = len(archive_domains_dataframe)
    total_successful_domains = total_live_domains + total_archive_domains

    global_coverage_percentage = (
        (total_successful_domains / total_processed_domains) * 100
        if total_processed_domains > 0
        else 0
    )

    def calculate_fill_rate(target_dataframe, target_column):
        total_in_dataframe = len(target_dataframe)
        if total_in_dataframe == 0:
            return 0.0
        filled_entries = len(
            target_dataframe[target_dataframe[target_column].map(len) > 0]
        )
        return (filled_entries / total_in_dataframe) * 100

    live_phone_fill_rate = calculate_fill_rate(
        live_domains_dataframe, "extracted_phones"
    )
    live_social_fill_rate = calculate_fill_rate(
        live_domains_dataframe, "extracted_social_links"
    )
    live_address_fill_rate = calculate_fill_rate(
        live_domains_dataframe, "extracted_addresses"
    )

    archive_phone_fill_rate = calculate_fill_rate(
        archive_domains_dataframe, "extracted_phones"
    )
    archive_social_fill_rate = calculate_fill_rate(
        archive_domains_dataframe, "extracted_social_links"
    )
    archive_address_fill_rate = calculate_fill_rate(
        archive_domains_dataframe, "extracted_addresses"
    )

    print("\n" + "=" * 60)
    print("CORPORATE DATA EXTRACTION AUDIT REPORT")
    print("=" * 60)
    print(f"Total Domains Analyzed : {total_processed_domains}")
    print(f"Total Execution Time   : {total_execution_time:.2f} seconds")
    print(f"Global Coverage Rate   : {global_coverage_percentage:.2f}%")
    print("-" * 60)
    print("[1] LIVE ENVIRONMENTS (ACTIVE SITES)")
    print("-" * 60)
    print(f"Live Domains Reached   : {total_live_domains}")
    print(f"Phone Fill Rate        : {live_phone_fill_rate:.2f}%")
    print(f"Social Fill Rate       : {live_social_fill_rate:.2f}%")
    print(f"Address Fill Rate      : {live_address_fill_rate:.2f}%")
    print("-" * 60)
    print("[2] ARCHIVED ENVIRONMENTS (WAYBACK Machine)")
    print("-" * 60)
    print(f"Archived Domains Found : {total_archive_domains}")
    print(f"Phone Fill Rate        : {archive_phone_fill_rate:.2f}%")
    print(f"Social Fill Rate       : {archive_social_fill_rate:.2f}%")
    print(f"Address Fill Rate      : {archive_address_fill_rate:.2f}%")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    try:
        input_dataframe = pd.read_csv("data/sample-websites.csv")
        list_of_target_domains = input_dataframe["domain"].dropna().tolist()
    except FileNotFoundError:
        list_of_target_domains = ["bostonzen.org", "mazautoglass.com", "timent.com"]

    print(
        f"\nInitializing Corporate Extraction Engine for {len(list_of_target_domains)} domains..."
    )
    process_start_time = time.time()

    final_scraping_results = asyncio.run(
        orchestrate_asynchronous_extraction(list_of_target_domains)
    )

    process_end_time = time.time()
    total_duration = process_end_time - process_start_time

    results_dataframe = pd.DataFrame(final_scraping_results)

    print_corporate_audit_report(results_dataframe, total_duration)

    results_dataframe.to_json("data/scraped_data.json", orient="records", indent=4)
