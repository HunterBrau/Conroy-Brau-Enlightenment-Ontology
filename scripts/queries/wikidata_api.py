from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import re

import pandas as pd


API_ENDPOINT = "https://www.wikidata.org/w/api.php"
USER_AGENT = "Chomputation/0.1 (Wikidata API helper)"
QID_PATTERN = re.compile(r"(Q\d+)$")
WIKIDATA_ENTITY_BASE = "http://www.wikidata.org/entity/"
DEFAULT_CHUNK_SIZE = 50


def normalize_qid(value) -> str | None:
    if pd.isna(value):
        return None
    match = QID_PATTERN.search(str(value).strip())
    if not match:
        return None
    return match.group(1)


def qid_uri(qid: str) -> str:
    return f"{WIKIDATA_ENTITY_BASE}{qid}"


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[start:start + size] for start in range(0, len(values), size)]


def fetch_entities(
    qids: set[str] | list[str],
    *,
    props: str = "labels|claims",
    languages: str = "fr|en",
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    user_agent: str = USER_AGENT,
    pause_seconds: float = 0.2,
) -> dict[str, dict]:
    sorted_qids = sorted(set(qids))
    entities = {}

    for qid_chunk in chunked(sorted_qids, chunk_size):
        parameters = {
            "action": "wbgetentities",
            "ids": "|".join(qid_chunk),
            "props": props,
            "languages": languages,
            "format": "json",
        }
        request = Request(
            f"{API_ENDPOINT}?{urlencode(parameters)}",
            headers={"User-Agent": user_agent},
            method="GET",
        )

        for attempt in range(1, 7):
            try:
                with urlopen(request, timeout=60) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                entities.update(payload.get("entities", {}))
                break
            except HTTPError as error:
                if attempt == 6 or error.code not in {429, 500, 502, 503, 504}:
                    body = error.read().decode("utf-8", errors="replace")
                    raise SystemExit(f"Wikidata API failed with HTTP {error.code}.\n{body}") from None
                retry_after = error.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep(int(retry_after))
                else:
                    sleep(attempt * 10)
            except URLError as error:
                if attempt == 6:
                    raise SystemExit(f"Could not reach the Wikidata API: {error}") from None
                sleep(attempt * 10)

        if pause_seconds:
            sleep(pause_seconds)

    return entities


def label_for(entity: dict, qid: str, languages: tuple[str, ...] = ("fr", "en")) -> str:
    labels = entity.get("labels", {})
    for language in languages:
        label = labels.get(language, {}).get("value")
        if label:
            return label
    return qid


def claim_qids(entity: dict, property_id: str) -> list[str]:
    qids = []
    seen = set()
    for claim in entity.get("claims", {}).get(property_id, []):
        mainsnak = claim.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        value = datavalue.get("value")
        if isinstance(value, dict) and value.get("id") and value["id"] not in seen:
            seen.add(value["id"])
            qids.append(value["id"])
    return qids


def first_coordinate(entity: dict) -> tuple[object, object]:
    for claim in entity.get("claims", {}).get("P625", []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(value, dict):
            return value.get("latitude", pd.NA), value.get("longitude", pd.NA)
    return pd.NA, pd.NA


def join_values(values: list[str]) -> object:
    cleaned = sorted({value for value in values if value})
    if not cleaned:
        return pd.NA
    return " | ".join(cleaned)


def join_ordered_values(values: list[str]) -> object:
    cleaned = []
    seen = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        cleaned.append(value)
    if not cleaned:
        return pd.NA
    return " | ".join(cleaned)
