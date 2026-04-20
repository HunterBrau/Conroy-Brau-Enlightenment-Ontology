from argparse import ArgumentParser
from pathlib import Path
import re

import pandas as pd


QID_PATTERN = re.compile(r"(Q\d+)$")

TEXT_ENRICHMENT_COLUMNS = [
    "wikidata_enrichment_label",
    "death_place_ids",
    "death_place_labels",
    "death_coords",
    "citizenship_ids",
    "citizenship_labels",
    "residence_ids",
    "residence_labels",
    "work_location_ids",
    "work_location_labels",
    "native_language_ids",
    "native_language_labels",
    "spoken_written_language_ids",
    "spoken_written_language_labels",
    "writing_language_ids",
    "writing_language_labels",
    "source_export_file",
]

BOOLEAN_ENRICHMENT_COLUMNS = [
    "has_frwiki",
    "has_enwiki",
    "has_itwiki",
    "has_dewiki",
    "has_eswiki",
    "has_plwiki",
    "has_ruwiki",
]

COUNT_ENRICHMENT_COLUMNS = [
    "wikipedia_sitelink_count",
]

EXPECTED_ENRICHMENT_COLUMNS = (
    TEXT_ENRICHMENT_COLUMNS
    + BOOLEAN_ENRICHMENT_COLUMNS
    + COUNT_ENRICHMENT_COLUMNS
)

EVIDENCE_FIELD_FLAGS = {
    "has_wikidata_death_place": [
        "death_place_ids",
        "death_place_labels",
        "death_coords",
    ],
    "has_wikidata_citizenship": [
        "citizenship_ids",
        "citizenship_labels",
    ],
    "has_wikidata_residence": [
        "residence_ids",
        "residence_labels",
    ],
    "has_wikidata_work_location": [
        "work_location_ids",
        "work_location_labels",
    ],
    "has_wikidata_native_language": [
        "native_language_ids",
        "native_language_labels",
    ],
    "has_wikidata_spoken_written_language": [
        "spoken_written_language_ids",
        "spoken_written_language_labels",
    ],
    "has_wikidata_writing_language": [
        "writing_language_ids",
        "writing_language_labels",
    ],
}


def normalize_blank_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(r"^\s*$", pd.NA, regex=True)


def normalize_path(project_root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    return project_root / path


def extract_qid(value) -> object:
    if pd.isna(value):
        return pd.NA

    match = QID_PATTERN.search(str(value).strip())
    if not match:
        return pd.NA

    return match.group(1)


def normalize_wikidata_id(value) -> object:
    qid = extract_qid(value)
    if pd.isna(qid):
        return pd.NA
    return f"http://www.wikidata.org/entity/{qid}"


def split_pipe_values(value) -> list[str]:
    if pd.isna(value):
        return []

    return [
        token.strip()
        for token in str(value).split("|")
        if token.strip()
    ]


def join_pipe_values(series: pd.Series) -> object:
    values = sorted(
        {
            token
            for value in series.dropna()
            for token in split_pipe_values(value)
        }
    )
    if not values:
        return pd.NA
    return " | ".join(values)


def collapse_boolean(series: pd.Series) -> object:
    normalized = series.dropna().astype("string").str.strip().str.lower()
    if normalized.empty:
        return pd.NA
    if normalized.isin(["true", "1", "yes"]).any():
        return True
    if normalized.isin(["false", "0", "no"]).any():
        return False
    return pd.NA


def normalize_boolean_value(value) -> object:
    if pd.isna(value):
        return pd.NA

    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    return pd.NA


def collapse_max_integer(series: pd.Series) -> object:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return pd.NA
    return int(numeric.max())


def resolve_input_paths(
    project_root: Path,
    input_args: list[str] | None,
    default_path: Path,
) -> list[Path]:
    candidate_paths = input_args or [str(default_path)]
    resolved_paths = []

    for raw_path in candidate_paths:
        path = normalize_path(project_root, Path(raw_path))
        if path.is_dir():
            resolved_paths.extend(sorted(path.glob("*.csv")))
        else:
            resolved_paths.append(path)

    missing_paths = [path for path in resolved_paths if not path.exists()]
    if missing_paths:
        missing_list = "\n".join(str(path) for path in missing_paths)
        raise FileNotFoundError(
            "Missing Wikidata enrichment export file(s):\n"
            f"{missing_list}\n\n"
            "Run the Step 04 Wikidata query, export the results as CSV, and "
            "save them as data/raw/wikidata_affiliation_enrichment.csv. "
            "If you ran chunked queries, pass each CSV export with --input "
            "or pass a directory containing the chunk CSV files."
        )

    if not resolved_paths:
        raise FileNotFoundError("No CSV files were found for Step 04 enrichment.")

    return resolved_paths


def read_enrichment_exports(input_paths: list[Path]) -> pd.DataFrame:
    frames = []
    for input_path in input_paths:
        df = pd.read_csv(input_path)
        df["source_export_file"] = input_path.name
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


def normalize_enrichment_df(enrichment_df: pd.DataFrame) -> pd.DataFrame:
    enrichment_df = enrichment_df.copy()
    enrichment_df.columns = [str(column).strip() for column in enrichment_df.columns]
    enrichment_df = normalize_blank_strings(enrichment_df)

    if "person" not in enrichment_df.columns:
        raise ValueError("Step 04 enrichment export must contain a 'person' column.")

    enrichment_df = enrichment_df.rename(
        columns={
            "person": "wikidata_id",
            "personLabel": "wikidata_enrichment_label",
        }
    )

    enrichment_df["wikidata_id"] = enrichment_df["wikidata_id"].apply(
        normalize_wikidata_id
    ).astype("string")

    for column in EXPECTED_ENRICHMENT_COLUMNS:
        if column not in enrichment_df.columns:
            enrichment_df[column] = pd.NA

    for column in TEXT_ENRICHMENT_COLUMNS:
        enrichment_df[column] = enrichment_df[column].astype("string").str.strip()

    for column in BOOLEAN_ENRICHMENT_COLUMNS:
        enrichment_df[column] = enrichment_df[column].apply(normalize_boolean_value)
        enrichment_df[column] = enrichment_df[column].astype("boolean")

    for column in COUNT_ENRICHMENT_COLUMNS:
        enrichment_df[column] = pd.to_numeric(
            enrichment_df[column],
            errors="coerce",
        ).astype("Int64")

    return enrichment_df[["wikidata_id"] + EXPECTED_ENRICHMENT_COLUMNS]


def summarize_enrichment_df(enrichment_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for wikidata_id, group in enrichment_df.groupby("wikidata_id", dropna=False):
        summary_row = {
            "wikidata_id": wikidata_id,
            "enrichment_source_row_count": len(group),
        }

        for column in TEXT_ENRICHMENT_COLUMNS:
            summary_row[column] = join_pipe_values(group[column])

        for column in BOOLEAN_ENRICHMENT_COLUMNS:
            summary_row[column] = collapse_boolean(group[column])

        for column in COUNT_ENRICHMENT_COLUMNS:
            summary_row[column] = collapse_max_integer(group[column])

        rows.append(summary_row)

    summary_df = pd.DataFrame(rows)
    for column in BOOLEAN_ENRICHMENT_COLUMNS:
        summary_df[column] = summary_df[column].astype("boolean")
    for column in COUNT_ENRICHMENT_COLUMNS + ["enrichment_source_row_count"]:
        summary_df[column] = pd.to_numeric(
            summary_df[column],
            errors="coerce",
        ).astype("Int64")

    return summary_df


def add_coverage_flags(enriched_df: pd.DataFrame) -> pd.DataFrame:
    enriched_df = enriched_df.copy()
    enriched_df["has_wikidata_enrichment"] = (
        enriched_df["enrichment_source_row_count"].notna()
    )

    for flag_column, source_columns in EVIDENCE_FIELD_FLAGS.items():
        available_columns = [
            column for column in source_columns
            if column in enriched_df.columns
        ]
        enriched_df[flag_column] = enriched_df[available_columns].notna().any(axis=1)

    enriched_df["has_wikidata_wikipedia_sitelinks"] = (
        pd.to_numeric(
            enriched_df["wikipedia_sitelink_count"],
            errors="coerce",
        ).fillna(0)
        > 0
    )

    evidence_flags = list(EVIDENCE_FIELD_FLAGS) + [
        "has_wikidata_wikipedia_sitelinks",
    ]
    enriched_df["wikidata_affiliation_evidence_fields_present"] = (
        enriched_df[evidence_flags].sum(axis=1).astype("Int64")
    )

    return enriched_df


def percentage(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)


def build_summary_table(
    base_df: pd.DataFrame,
    enrichment_raw_df: pd.DataFrame,
    enrichment_summary_df: pd.DataFrame,
    enriched_df: pd.DataFrame,
) -> pd.DataFrame:
    total_rows = len(base_df)
    total_entities = int(base_df["wikidata_id"].nunique(dropna=True))
    matched_mask = enriched_df["has_wikidata_enrichment"]
    duplicate_raw_mask = enrichment_raw_df.duplicated("wikidata_id", keep=False)
    base_ids = set(base_df["wikidata_id"].dropna().astype(str))
    enrichment_ids = set(enrichment_summary_df["wikidata_id"].dropna().astype(str))
    extra_enrichment_entities = len(enrichment_ids - base_ids)

    summary_rows = [
        ("total_rows", total_rows, "rows"),
        ("distinct_wikidata_entities", total_entities, "entities"),
        ("raw_enrichment_rows", len(enrichment_raw_df), pd.NA),
        (
            "normalized_enrichment_entities",
            int(enrichment_summary_df["wikidata_id"].nunique(dropna=True)),
            "entities",
        ),
        (
            "duplicate_raw_enrichment_rows",
            int(duplicate_raw_mask.sum()),
            pd.NA,
        ),
        (
            "extra_enrichment_entities",
            extra_enrichment_entities,
            pd.NA,
        ),
        (
            "rows_with_wikidata_enrichment",
            int(matched_mask.sum()),
            "rows",
        ),
        (
            "entities_with_wikidata_enrichment",
            int(enriched_df.loc[matched_mask, "wikidata_id"].nunique(dropna=True)),
            "entities",
        ),
        (
            "rows_missing_wikidata_enrichment",
            int((~matched_mask).sum()),
            "rows",
        ),
        (
            "entities_missing_wikidata_enrichment",
            int(enriched_df.loc[~matched_mask, "wikidata_id"].nunique(dropna=True)),
            "entities",
        ),
    ]

    summary_df = pd.DataFrame(summary_rows, columns=["metric", "value", "pct_base"])
    summary_df["pct_of_rows"] = summary_df.apply(
        lambda row: percentage(int(row["value"]), total_rows)
        if row["pct_base"] == "rows"
        else pd.NA,
        axis=1,
    )
    summary_df["pct_of_entities"] = summary_df.apply(
        lambda row: percentage(int(row["value"]), total_entities)
        if row["pct_base"] == "entities"
        else pd.NA,
        axis=1,
    )
    return summary_df.drop(columns=["pct_base"])


def build_field_coverage_table(enriched_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    total_rows = len(enriched_df)
    total_entities = int(enriched_df["wikidata_id"].nunique(dropna=True))
    coverage_flags = list(EVIDENCE_FIELD_FLAGS) + [
        "has_wikidata_wikipedia_sitelinks",
    ]

    for flag_column in coverage_flags:
        mask = enriched_df[flag_column].fillna(False)
        rows.append(
            {
                "field": flag_column.replace("has_wikidata_", ""),
                "row_count": int(mask.sum()),
                "row_pct": percentage(int(mask.sum()), total_rows),
                "entity_count": int(
                    enriched_df.loc[mask, "wikidata_id"].nunique(dropna=True)
                ),
                "entity_pct": percentage(
                    int(enriched_df.loc[mask, "wikidata_id"].nunique(dropna=True)),
                    total_entities,
                ),
            }
        )

    return pd.DataFrame(rows)


def parse_args() -> object:
    parser = ArgumentParser(
        description="Merge Step 04 Wikidata enrichment exports into the cohort."
    )
    parser.add_argument(
        "--input",
        action="append",
        dest="input_paths",
        help=(
            "Path to a Wikidata CSV export, or a directory of CSV exports. "
            "Use this option more than once for multiple chunk files. "
            "Default: data/raw/wikidata_affiliation_enrichment.csv"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]

    base_path = project_root / "data" / "interim" / "writers_cleaned.csv"
    default_input_path = (
        project_root / "data" / "raw" / "wikidata_affiliation_enrichment.csv"
    )
    output_path = (
        project_root / "data" / "interim" / "writers_wikidata_enriched.csv"
    )
    missing_entities_path = (
        project_root / "data" / "interim" / "wikidata_enrichment_missing_entities.csv"
    )
    extra_entities_path = (
        project_root / "data" / "interim" / "wikidata_enrichment_extra_entities.csv"
    )
    duplicate_rows_path = (
        project_root / "data" / "interim" / "wikidata_enrichment_duplicate_rows.csv"
    )
    summary_path = (
        project_root / "data" / "interim" / "wikidata_enrichment_summary.csv"
    )
    field_coverage_path = (
        project_root / "data" / "interim" / "wikidata_enrichment_field_coverage.csv"
    )

    if not base_path.exists():
        raise SystemExit(
            "Missing cleaned dataset. Run Step 01 and Step 02 first:\n"
            "python scripts/pipeline/01_build_merged_dataset.py\n"
            "python scripts/pipeline/02_clean_structural_fields.py"
        )

    try:
        input_paths = resolve_input_paths(
            project_root=project_root,
            input_args=args.input_paths,
            default_path=default_input_path,
        )

        base_df = pd.read_csv(base_path)
        base_df["wikidata_id"] = base_df["wikidata_id"].astype("string").str.strip()

        enrichment_raw_df = read_enrichment_exports(input_paths)
        enrichment_df = normalize_enrichment_df(enrichment_raw_df)
    except (FileNotFoundError, ValueError) as error:
        raise SystemExit(str(error)) from None

    enrichment_summary_df = summarize_enrichment_df(enrichment_df)

    duplicate_rows_df = enrichment_df.loc[
        enrichment_df.duplicated("wikidata_id", keep=False)
    ].sort_values(["wikidata_id", "source_export_file"], na_position="last")

    enriched_df = base_df.merge(
        enrichment_summary_df,
        on="wikidata_id",
        how="left",
        validate="many_to_one",
    )
    enriched_df = add_coverage_flags(enriched_df)

    base_entity_df = (
        base_df.drop_duplicates("wikidata_id")
        .loc[
            :,
            [
                "wikidata_id",
                "name",
                "birth_year",
                "birth_place",
                "occupation_raw",
            ],
        ]
        .copy()
    )
    matched_ids = set(enrichment_summary_df["wikidata_id"].dropna().astype(str))
    missing_entities_df = base_entity_df.loc[
        ~base_entity_df["wikidata_id"].astype(str).isin(matched_ids)
    ]

    base_ids = set(base_df["wikidata_id"].dropna().astype(str))
    extra_entities_df = enrichment_summary_df.loc[
        ~enrichment_summary_df["wikidata_id"].astype(str).isin(base_ids)
    ]

    summary_df = build_summary_table(
        base_df=base_df,
        enrichment_raw_df=enrichment_df,
        enrichment_summary_df=enrichment_summary_df,
        enriched_df=enriched_df,
    )
    field_coverage_df = build_field_coverage_table(enriched_df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    enriched_df.to_csv(output_path, index=False)
    missing_entities_df.to_csv(missing_entities_path, index=False)
    extra_entities_df.to_csv(extra_entities_path, index=False)
    duplicate_rows_df.to_csv(duplicate_rows_path, index=False)
    summary_df.to_csv(summary_path, index=False)
    field_coverage_df.to_csv(field_coverage_path, index=False)

    print("Wikidata enrichment merge complete.")
    print(f"Input export files: {len(input_paths)}")
    print(f"Output dataset: {output_path}")
    print(f"Summary file: {summary_path}")
    print(f"Field coverage file: {field_coverage_path}")
    print()
    print(f"Rows: {len(enriched_df)}")
    print(
        "Rows with Wikidata enrichment: "
        f"{int(enriched_df['has_wikidata_enrichment'].sum())}"
    )
    print(
        "Rows missing Wikidata enrichment: "
        f"{int((~enriched_df['has_wikidata_enrichment']).sum())}"
    )


if __name__ == "__main__":
    main()
