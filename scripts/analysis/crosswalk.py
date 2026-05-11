from pathlib import Path

import pandas as pd


CROSSWALK_PATH = Path("data/reference/political_entity_affiliation_crosswalk_seed.csv")

BOOLEAN_COLUMNS = [
    "include_in_imperial_context",
    "include_in_modern_country_rollup",
    "include_in_europe_binary",
    "include_in_non_europe_or_colonial",
]

MANUAL_REVIEW_SORT_ORDER = {
    "high": 3,
    "medium": 2,
    "low": 1,
}


def normalize_bool(value) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def load_political_crosswalk(project_root: Path) -> pd.DataFrame:
    path = project_root / CROSSWALK_PATH
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path).replace(r"^\s*$", pd.NA, regex=True)
    if "manual_review_label" not in df.columns and "confidence" in df.columns:
        df = df.rename(columns={"confidence": "manual_review_label"})
    for column in BOOLEAN_COLUMNS:
        if column in df.columns:
            df[column] = df[column].apply(normalize_bool)
    df["manual_review_rank"] = (
        df["manual_review_label"]
        .astype(str)
        .str.strip()
        .str.lower()
        .map(MANUAL_REVIEW_SORT_ORDER)
        .fillna(0)
        .astype(int)
    )
    df["wikidata_url"] = "https://www.wikidata.org/wiki/" + df["wikidata_id"].astype(str)
    return df


def load_country_affiliation_mapping(project_root: Path, fallback_mapping: dict[str, str]) -> dict[str, str]:
    mapping = dict(fallback_mapping)
    crosswalk_df = load_political_crosswalk(project_root)
    if crosswalk_df.empty:
        return mapping

    reviewed_mapping = dict(
        zip(
            crosswalk_df["wikidata_id"].astype(str),
            crosswalk_df["review_group"].astype(str),
            strict=False,
        )
    )
    mapping.update(reviewed_mapping)
    return mapping


def load_token_label_overrides(project_root: Path, fallback_mapping: dict[str, str]) -> dict[str, str]:
    mapping = dict(fallback_mapping)
    crosswalk_df = load_political_crosswalk(project_root)
    if crosswalk_df.empty:
        return mapping

    reviewed_labels = dict(
        zip(
            crosswalk_df["wikidata_id"].astype(str),
            crosswalk_df["label"].astype(str),
            strict=False,
        )
    )
    mapping.update(reviewed_labels)
    return mapping


def build_reviewed_scope_sets(
    project_root: Path,
    *,
    fallback_china_ids: set[str],
    fallback_british_empire_ids: set[str],
    fallback_non_europe_ids: set[str],
    fallback_colonial_ids: set[str],
    fallback_transcontinental_ids: set[str],
    fallback_europe_ids: set[str],
) -> dict[str, set[str]]:
    crosswalk_df = load_political_crosswalk(project_root)
    if crosswalk_df.empty:
        return {
            "china_context_ids": set(fallback_china_ids),
            "british_empire_context_ids": set(fallback_british_empire_ids),
            "non_europe_geographic_ids": set(fallback_non_europe_ids),
            "colonial_context_ids": set(fallback_colonial_ids),
            "transcontinental_or_imperial_ids": set(fallback_transcontinental_ids),
            "europe_geographic_ids": set(fallback_europe_ids),
        }

    qids = crosswalk_df["wikidata_id"].astype(str)
    china_ids = set(crosswalk_df.loc[crosswalk_df["review_group"] == "China", "wikidata_id"].astype(str))
    british_empire_ids = set(
        crosswalk_df.loc[
            (crosswalk_df["review_group"] == "British")
            & (crosswalk_df["include_in_imperial_context"]),
            "wikidata_id",
        ].astype(str)
    )
    non_europe_ids = set(
        crosswalk_df.loc[crosswalk_df["include_in_non_europe_or_colonial"], "wikidata_id"].astype(str)
    )
    colonial_ids = set(
        crosswalk_df.loc[
            crosswalk_df["include_in_non_europe_or_colonial"]
            & (
                crosswalk_df["include_in_imperial_context"]
                | crosswalk_df["context_type"].astype(str).str.contains("colonial", case=False, na=False)
            ),
            "wikidata_id",
        ].astype(str)
    )
    europe_ids = set(crosswalk_df.loc[crosswalk_df["include_in_europe_binary"], "wikidata_id"].astype(str))
    transcontinental_ids = set(
        crosswalk_df.loc[
            crosswalk_df["include_in_imperial_context"]
            & ~crosswalk_df["include_in_non_europe_or_colonial"]
            & ~qids.isin(british_empire_ids),
            "wikidata_id",
        ].astype(str)
    )

    return {
        "china_context_ids": set(fallback_china_ids) | china_ids,
        "british_empire_context_ids": set(fallback_british_empire_ids) | british_empire_ids,
        "non_europe_geographic_ids": set(fallback_non_europe_ids) | non_europe_ids,
        "colonial_context_ids": set(fallback_colonial_ids) | colonial_ids,
        "transcontinental_or_imperial_ids": set(fallback_transcontinental_ids) | transcontinental_ids,
        "europe_geographic_ids": set(fallback_europe_ids) | europe_ids,
    }
