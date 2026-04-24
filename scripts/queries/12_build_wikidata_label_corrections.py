from argparse import ArgumentParser
from pathlib import Path
import json
import re
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


API_ENDPOINT = "https://www.wikidata.org/w/api.php"
USER_AGENT = "Chomputation/0.1 (Wikidata label correction helper)"
QID_PATTERN = re.compile(r"(Q\d+)$")
PREFERRED_LABEL_LANGUAGES = [
    "fr",
    "en",
    "de",
    "it",
    "es",
    "nl",
    "pl",
    "ru",
    "pt",
    "sv",
    "da",
    "la",
]


def extract_qid(value) -> object:
    if pd.isna(value):
        return pd.NA

    match = QID_PATTERN.search(str(value).strip())
    if not match:
        return pd.NA

    return match.group(1)


def bool_mask(series: pd.Series) -> pd.Series:
    return series.astype("string").str.lower().eq("true").fillna(False)


def unique_join(series: pd.Series) -> object:
    values = sorted(
        {
            str(value).strip()
            for value in series.dropna()
            if str(value).strip()
        }
    )
    if not values:
        return pd.NA
    return " | ".join(values)


def chunked(values: list[str], chunk_size: int) -> list[list[str]]:
    return [
        values[start:start + chunk_size]
        for start in range(0, len(values), chunk_size)
    ]


def fetch_entity_labels(qids: list[str], chunk_size: int = 50) -> dict[str, dict]:
    entities: dict[str, dict] = {}

    for qid_chunk in chunked(sorted(set(qids)), chunk_size):
        query = urlencode(
            {
                "action": "wbgetentities",
                "ids": "|".join(qid_chunk),
                "props": "labels|descriptions",
                "format": "json",
                "formatversion": "2",
            }
        )
        request = Request(
            f"{API_ENDPOINT}?{query}",
            headers={"User-Agent": USER_AGENT},
        )

        try:
            with urlopen(request, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            raise SystemExit(
                f"Wikidata label query failed with HTTP {error.code}.\n{body}"
            ) from None
        except URLError as error:
            raise SystemExit(f"Could not reach Wikidata: {error}") from None

        if payload.get("success") != 1:
            raise SystemExit(f"Wikidata label query failed: {payload}")

        entities.update(payload.get("entities", {}))

    return entities


def value_for_language(entity: dict, field: str, language: str) -> object:
    values = entity.get(field, {})
    language_record = values.get(language)
    if not language_record:
        return pd.NA
    return language_record.get("value", pd.NA)


def choose_label(entity: dict) -> tuple[object, object]:
    labels = entity.get("labels", {})

    for language in PREFERRED_LABEL_LANGUAGES:
        language_record = labels.get(language)
        if language_record and language_record.get("value"):
            return language_record["value"], language

    for language in sorted(labels):
        language_record = labels[language]
        if language_record.get("value"):
            return language_record["value"], language

    return pd.NA, pd.NA


def build_person_corrections(
    df: pd.DataFrame,
    entities: dict[str, dict],
) -> pd.DataFrame:
    unresolved = df.loc[bool_mask(df["name_is_qid"])].copy()
    if unresolved.empty:
        return pd.DataFrame(
            columns=[
                "wikidata_id",
                "person_qid",
                "original_name",
                "resolved_name",
                "resolved_label_language",
                "label_fr",
                "label_en",
                "description_fr",
                "description_en",
                "affected_row_count",
                "birth_year_values",
                "birth_place_values",
                "label_source",
            ]
        )

    unresolved["person_qid"] = unresolved["wikidata_id"].apply(extract_qid)
    summary = (
        unresolved.groupby(["wikidata_id", "person_qid", "name"], dropna=False)
        .agg(
            affected_row_count=("wikidata_id", "size"),
            birth_year_values=("birth_year", unique_join),
            birth_place_values=("birth_place", unique_join),
        )
        .reset_index()
        .rename(columns={"name": "original_name"})
    )

    rows = []
    for row in summary.itertuples(index=False):
        entity = entities.get(row.person_qid, {})
        resolved_label, resolved_language = choose_label(entity)
        rows.append(
            {
                "wikidata_id": row.wikidata_id,
                "person_qid": row.person_qid,
                "original_name": row.original_name,
                "resolved_name": resolved_label,
                "resolved_label_language": resolved_language,
                "label_fr": value_for_language(entity, "labels", "fr"),
                "label_en": value_for_language(entity, "labels", "en"),
                "description_fr": value_for_language(entity, "descriptions", "fr"),
                "description_en": value_for_language(entity, "descriptions", "en"),
                "affected_row_count": row.affected_row_count,
                "birth_year_values": row.birth_year_values,
                "birth_place_values": row.birth_place_values,
                "label_source": "wikidata_wbgetentities",
            }
        )

    return pd.DataFrame(rows).sort_values(["resolved_name", "person_qid"])


def build_birth_place_corrections(
    df: pd.DataFrame,
    entities: dict[str, dict],
) -> pd.DataFrame:
    unresolved = df.loc[bool_mask(df["birth_place_is_qid"])].copy()
    if unresolved.empty:
        return pd.DataFrame(
            columns=[
                "birth_place_qid",
                "original_birth_place",
                "resolved_birth_place",
                "resolved_label_language",
                "label_fr",
                "label_en",
                "description_fr",
                "description_en",
                "affected_row_count",
                "affected_entity_count",
                "example_wikidata_ids",
                "example_names",
                "label_source",
            ]
        )

    unresolved["birth_place_qid"] = unresolved["birth_place"].apply(extract_qid)
    summary = (
        unresolved.groupby(["birth_place_qid", "birth_place"], dropna=False)
        .agg(
            affected_row_count=("wikidata_id", "size"),
            affected_entity_count=("wikidata_id", "nunique"),
            example_wikidata_ids=("wikidata_id", unique_join),
            example_names=("name", unique_join),
        )
        .reset_index()
        .rename(columns={"birth_place": "original_birth_place"})
    )

    rows = []
    for row in summary.itertuples(index=False):
        entity = entities.get(row.birth_place_qid, {})
        resolved_label, resolved_language = choose_label(entity)
        rows.append(
            {
                "birth_place_qid": row.birth_place_qid,
                "original_birth_place": row.original_birth_place,
                "resolved_birth_place": resolved_label,
                "resolved_label_language": resolved_language,
                "label_fr": value_for_language(entity, "labels", "fr"),
                "label_en": value_for_language(entity, "labels", "en"),
                "description_fr": value_for_language(entity, "descriptions", "fr"),
                "description_en": value_for_language(entity, "descriptions", "en"),
                "affected_row_count": row.affected_row_count,
                "affected_entity_count": row.affected_entity_count,
                "example_wikidata_ids": row.example_wikidata_ids,
                "example_names": row.example_names,
                "label_source": "wikidata_wbgetentities",
            }
        )

    return pd.DataFrame(rows).sort_values(["resolved_birth_place", "birth_place_qid"])


def parse_args() -> object:
    parser = ArgumentParser(
        description="Build correction tables for unresolved Wikidata QID labels."
    )
    parser.add_argument(
        "--input",
        default="data/interim/writers_cleaned.csv",
        help="Cleaned cohort CSV. Default: data/interim/writers_cleaned.csv",
    )
    parser.add_argument(
        "--person-output",
        default="data/processed/person_name_label_corrections.csv",
        help=(
            "Output CSV for unresolved person-name labels. "
            "Default: data/processed/person_name_label_corrections.csv"
        ),
    )
    parser.add_argument(
        "--birth-place-output",
        default="data/processed/birth_place_label_corrections.csv",
        help=(
            "Output CSV for unresolved birth-place labels. "
            "Default: data/processed/birth_place_label_corrections.csv"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]

    input_path = project_root / args.input
    person_output_path = project_root / args.person_output
    birth_place_output_path = project_root / args.birth_place_output

    if not input_path.exists():
        raise SystemExit(
            f"Missing cleaned cohort: {input_path}\n"
            "Run Steps 01 and 02 first."
        )

    df = pd.read_csv(input_path)
    required_columns = {
        "wikidata_id",
        "name",
        "name_is_qid",
        "birth_year",
        "birth_place",
        "birth_place_is_qid",
    }
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        raise SystemExit(
            "Missing required columns in cleaned cohort: "
            + ", ".join(missing_columns)
        )

    person_qids = [
        qid
        for qid in df.loc[bool_mask(df["name_is_qid"]), "wikidata_id"].apply(extract_qid)
        if pd.notna(qid)
    ]
    birth_place_qids = [
        qid
        for qid in df.loc[
            bool_mask(df["birth_place_is_qid"]), "birth_place"
        ].apply(extract_qid)
        if pd.notna(qid)
    ]
    qids = sorted(set(person_qids + birth_place_qids))
    entities = fetch_entity_labels(qids) if qids else {}

    person_corrections_df = build_person_corrections(df, entities)
    birth_place_corrections_df = build_birth_place_corrections(df, entities)

    person_output_path.parent.mkdir(parents=True, exist_ok=True)
    birth_place_output_path.parent.mkdir(parents=True, exist_ok=True)
    person_corrections_df.to_csv(person_output_path, index=False)
    birth_place_corrections_df.to_csv(birth_place_output_path, index=False)

    print("Wikidata label correction tables complete.")
    print(f"Person-name corrections: {person_output_path}")
    print(f"Birth-place corrections: {birth_place_output_path}")
    print()
    print(f"Unresolved person-name rows: {int(bool_mask(df['name_is_qid']).sum())}")
    print(f"Person correction rows: {len(person_corrections_df)}")
    print(
        "Unresolved birth-place rows: "
        f"{int(bool_mask(df['birth_place_is_qid']).sum())}"
    )
    print(f"Birth-place correction rows: {len(birth_place_corrections_df)}")


if __name__ == "__main__":
    main()
