from pathlib import Path

import pandas as pd


EUROPEAN_LANGUAGE_CODES = ["fr", "en", "de", "it", "es", "pl", "ru", "uk", "nl", "pt", "sv", "da"]

WIKI_COLUMNS = {
    "frwiki": "has_frwiki",
    "enwiki": "has_enwiki",
    "dewiki": "has_dewiki",
    "itwiki": "has_itwiki",
    "eswiki": "has_eswiki",
    "plwiki": "has_plwiki",
    "ruwiki": "has_ruwiki",
    "ukwiki": "has_ukwiki",
    "nlwiki": "has_nlwiki",
    "ptwiki": "has_ptwiki",
    "svwiki": "has_svwiki",
    "dawiki": "has_dawiki",
}


def normalize_blank_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(r"^\s*$", pd.NA, regex=True)


def split_pipe_values(value) -> list[str]:
    if pd.isna(value):
        return []
    return [token.strip() for token in str(value).split("|") if token.strip()]


def ordered_unique(values) -> list[str]:
    seen = set()
    cleaned = []
    for value in values:
        if pd.isna(value):
            continue
        token = str(value).strip()
        if token and token not in seen:
            seen.add(token)
            cleaned.append(token)
    return cleaned


def join_values(values, *, sort: bool = False) -> object:
    cleaned = ordered_unique(values)
    if sort:
        cleaned = sorted(cleaned)
    if not cleaned:
        return pd.NA
    return " | ".join(cleaned)


def join_example_values(values, *, limit: int = 8) -> str:
    cleaned = sorted(ordered_unique(values))
    return " | ".join(cleaned[:limit])


def percentage(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)


def ratio(numerator: int, denominator: int) -> object:
    if denominator == 0:
        return pd.NA
    return round(numerator / denominator, 4)


def qid_from_uri(value) -> object:
    if pd.isna(value):
        return pd.NA
    token = str(value).strip().rstrip("/")
    if not token:
        return pd.NA
    return token.rsplit("/", 1)[-1]


def qid_uri(qid: str) -> str:
    return f"http://www.wikidata.org/entity/{qid}"


def qid_url(qid: str) -> str:
    return f"https://www.wikidata.org/wiki/{qid}"


def bool_value(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)
