from pathlib import Path

import pandas as pd


TOP_N = 15
EXAMPLE_N = 10


def percentage(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)


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


def bool_mask(series: pd.Series) -> pd.Series:
    return series.astype("string").str.lower().eq("true").fillna(False)


def build_missingness_table(df: pd.DataFrame) -> pd.DataFrame:
    total_rows = len(df)
    rows = []

    for column in df.columns:
        missing_count = int(df[column].isna().sum())
        non_missing_count = total_rows - missing_count
        rows.append(
            {
                "column": column,
                "non_missing_count": non_missing_count,
                "missing_count": missing_count,
                "missing_pct": percentage(missing_count, total_rows),
                "distinct_non_null_values": int(df[column].nunique(dropna=True)),
            }
        )

    return pd.DataFrame(rows).sort_values(
        by=["missing_count", "column"],
        ascending=[False, True],
    )


def build_distribution_table(
    df: pd.DataFrame,
    column: str,
    label: str,
) -> pd.DataFrame:
    total_rows = len(df)
    total_entities = int(df["wikidata_id"].nunique(dropna=True))

    grouped = (
        df.groupby(column, dropna=False)
        .agg(
            row_count=("wikidata_id", "size"),
            distinct_entities=("wikidata_id", "nunique"),
        )
        .reset_index()
        .rename(columns={column: label})
    )

    grouped[label] = grouped[label].fillna("[missing]")
    grouped["row_pct"] = grouped["row_count"].apply(
        lambda count: percentage(int(count), total_rows)
    )
    grouped["entity_pct"] = grouped["distinct_entities"].apply(
        lambda count: percentage(int(count), total_entities)
    )

    return grouped.sort_values(
        by=["row_count", "distinct_entities", label],
        ascending=[False, False, True],
    )


def build_duplicate_entity_table(df: pd.DataFrame) -> pd.DataFrame:
    duplicate_rows = df.loc[bool_mask(df["has_duplicate_wikidata_id"])].copy()
    if duplicate_rows.empty:
        return pd.DataFrame(
            columns=[
                "wikidata_id",
                "name",
                "duplicate_row_count",
                "birth_year_values",
                "birth_year_from_date_values",
                "birth_place_values",
                "viaf_id_values",
            ]
        )

    summary = (
        duplicate_rows.groupby(["wikidata_id", "name"], dropna=False)
        .agg(
            duplicate_row_count=("duplicate_row_count", "max"),
            birth_year_values=("birth_year", unique_join),
            birth_year_from_date_values=("birth_year_from_date", unique_join),
            birth_place_values=("birth_place", unique_join),
            viaf_id_values=("viaf_id", unique_join),
        )
        .reset_index()
    )

    return summary.sort_values(
        by=["duplicate_row_count", "name", "wikidata_id"],
        ascending=[False, True, True],
    )


def build_viaf_conflict_entity_table(
    df: pd.DataFrame,
    raw_conflicts_df: pd.DataFrame,
) -> pd.DataFrame:
    viaf_conflict_rows = df.loc[bool_mask(df["viaf_has_conflict"])].copy()
    if viaf_conflict_rows.empty:
        return pd.DataFrame(
            columns=[
                "wikidata_id",
                "name",
                "row_count",
                "raw_conflict_row_count",
                "viaf_record_count",
                "viaf_birth_date_count",
                "viaf_id_count",
                "birth_year_values",
                "birth_date_candidates",
                "viaf_id_candidates",
            ]
        )

    raw_conflict_counts = (
        raw_conflicts_df.groupby("wikidata_id")
        .size()
        .rename("raw_conflict_row_count")
        .reset_index()
        if not raw_conflicts_df.empty
        else pd.DataFrame(columns=["wikidata_id", "raw_conflict_row_count"])
    )

    summary = (
        viaf_conflict_rows.groupby(["wikidata_id", "name"], dropna=False)
        .agg(
            row_count=("wikidata_id", "size"),
            viaf_record_count=("viaf_record_count", "max"),
            viaf_birth_date_count=("viaf_birth_date_count", "max"),
            viaf_id_count=("viaf_id_count", "max"),
            birth_year_values=("birth_year", unique_join),
            birth_date_candidates=("birth_date_candidates", unique_join),
            viaf_id_candidates=("viaf_id_candidates", unique_join),
        )
        .reset_index()
        .merge(raw_conflict_counts, on="wikidata_id", how="left")
    )

    summary["raw_conflict_row_count"] = (
        pd.to_numeric(summary["raw_conflict_row_count"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    return summary.sort_values(
        by=[
            "raw_conflict_row_count",
            "viaf_birth_date_count",
            "viaf_id_count",
            "name",
        ],
        ascending=[False, False, False, True],
    )


def build_summary_table(
    df: pd.DataFrame,
    raw_conflicts_df: pd.DataFrame,
) -> pd.DataFrame:
    total_rows = len(df)
    distinct_entities = int(df["wikidata_id"].nunique(dropna=True))
    duplicate_group_sizes = df.groupby("wikidata_id", dropna=False).size()
    duplicate_entity_sizes = duplicate_group_sizes.loc[duplicate_group_sizes > 1]
    duplicate_entities = int(len(duplicate_entity_sizes))
    rows_in_duplicate_entities = int(duplicate_entity_sizes.sum())
    surplus_duplicate_rows = int((duplicate_entity_sizes - 1).sum())

    viaf_conflict_mask = bool_mask(df["viaf_has_conflict"])
    strict_mismatch_mask = bool_mask(df["birth_year_mismatch"])
    candidate_ambiguity_mask = bool_mask(df["birth_year_candidate_ambiguity_exists"])
    candidate_support_gap_mask = bool_mask(df["birth_year_without_candidate_support"])
    unresolved_name_mask = bool_mask(df["name_is_qid"])
    unresolved_birth_place_mask = bool_mask(df["birth_place_is_qid"])
    coords_missing_mask = df["birth_lon"].isna() | df["birth_lat"].isna()

    summary_rows = [
        ("total_rows", total_rows, "rows"),
        ("distinct_wikidata_entities", distinct_entities, "entities"),
        ("rows_in_duplicate_entities", rows_in_duplicate_entities, "rows"),
        ("surplus_duplicate_rows", surplus_duplicate_rows, "rows"),
        ("duplicate_entities", duplicate_entities, "entities"),
        ("rows_with_unambiguous_viaf_id", int(df["viaf_id"].notna().sum()), "rows"),
        (
            "entities_with_unambiguous_viaf_id",
            int(df.loc[df["viaf_id"].notna(), "wikidata_id"].nunique(dropna=True)),
            "entities",
        ),
        (
            "rows_with_ambiguous_viaf_metadata",
            int(viaf_conflict_mask.sum()),
            "rows",
        ),
        (
            "entities_with_ambiguous_viaf_metadata",
            int(df.loc[viaf_conflict_mask, "wikidata_id"].nunique(dropna=True)),
            "entities",
        ),
        ("raw_viaf_conflict_rows", int(len(raw_conflicts_df)), pd.NA),
        (
            "rows_with_unambiguous_birth_date",
            int(df["birth_date"].notna().sum()),
            "rows",
        ),
        (
            "entities_with_unambiguous_birth_date",
            int(df.loc[df["birth_date"].notna(), "wikidata_id"].nunique(dropna=True)),
            "entities",
        ),
        ("rows_with_birth_year_mismatch", int(strict_mismatch_mask.sum()), "rows"),
        (
            "rows_with_birth_year_candidate_ambiguity",
            int(candidate_ambiguity_mask.sum()),
            "rows",
        ),
        (
            "rows_without_birth_year_candidate_support",
            int(candidate_support_gap_mask.sum()),
            "rows",
        ),
        (
            "rows_with_unresolved_name_label",
            int(unresolved_name_mask.sum()),
            "rows",
        ),
        (
            "rows_with_unresolved_birth_place_label",
            int(unresolved_birth_place_mask.sum()),
            "rows",
        ),
        ("rows_missing_birth_place", int(df["birth_place"].isna().sum()), "rows"),
        (
            "rows_missing_parsed_coordinates",
            int(coords_missing_mask.sum()),
            "rows",
        ),
        ("unique_birth_places", int(df["birth_place"].nunique(dropna=True)), pd.NA),
        (
            "unique_occupation_labels",
            int(df["occupation_raw"].nunique(dropna=True)),
            pd.NA,
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
        lambda row: percentage(int(row["value"]), distinct_entities)
        if row["pct_base"] == "entities"
        else pd.NA,
        axis=1,
    )
    summary_df = summary_df.drop(columns=["pct_base"])
    return summary_df


def build_report(
    summary_df: pd.DataFrame,
    manifest_df: pd.DataFrame,
) -> str:
    metric_lookup = {
        row.metric: row.value
        for row in summary_df.itertuples(index=False)
    }
    manifest_lines = [
        f"- `{row.path}`: {row.description}"
        for row in manifest_df.itertuples(index=False)
    ]

    lines = [
        "# Dataset Diagnostics",
        "",
        "## Overview",
        "",
        (
            f"- Rows: {metric_lookup['total_rows']}"
        ),
        (
            f"- Distinct Wikidata entities: "
            f"{metric_lookup['distinct_wikidata_entities']}"
        ),
        (
            f"- Rows in duplicate entities: "
            f"{metric_lookup['rows_in_duplicate_entities']} across "
            f"{metric_lookup['duplicate_entities']} entities"
        ),
        (
            f"- Surplus duplicate rows beyond first occurrence: "
            f"{metric_lookup['surplus_duplicate_rows']}"
        ),
        (
            f"- Unambiguous VIAF coverage: "
            f"{metric_lookup['rows_with_unambiguous_viaf_id']} rows"
        ),
        (
            f"- Ambiguous VIAF metadata: "
            f"{metric_lookup['rows_with_ambiguous_viaf_metadata']} rows across "
            f"{metric_lookup['entities_with_ambiguous_viaf_metadata']} entities"
        ),
        (
            f"- Birth-year candidate ambiguity: "
            f"{metric_lookup['rows_with_birth_year_candidate_ambiguity']} rows"
        ),
        (
            f"- Rows without birth-year support from candidates: "
            f"{metric_lookup['rows_without_birth_year_candidate_support']}"
        ),
        (
            f"- Unresolved name labels: "
            f"{metric_lookup['rows_with_unresolved_name_label']}"
        ),
        (
            f"- Unresolved birth-place labels: "
            f"{metric_lookup['rows_with_unresolved_birth_place_label']}"
        ),
        "",
        "## How to Read This",
        "",
        "The CSV files are the canonical diagnostic outputs. This Markdown file",
        "is only a short orientation note for collaborators.",
        "",
        "## Diagnostic CSV Files",
        "",
        *manifest_lines,
        "",
    ]
    return "\n".join(lines)


def build_manifest_table(
    project_root: Path,
    output_descriptions: list[tuple[Path, str]],
) -> pd.DataFrame:
    rows = []
    for path, description in output_descriptions:
        rows.append(
            {
                "path": path.relative_to(project_root).as_posix(),
                "description": description,
                "format": path.suffix.lstrip("."),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]

    cleaned_path = project_root / "data" / "interim" / "writers_cleaned.csv"
    raw_conflicts_path = project_root / "data" / "interim" / "viaf_conflicts.csv"

    summary_path = project_root / "data" / "interim" / "dataset_diagnostics_summary.csv"
    missingness_path = (
        project_root / "data" / "interim" / "dataset_diagnostics_missingness.csv"
    )
    occupation_path = (
        project_root
        / "data"
        / "interim"
        / "dataset_diagnostics_occupation_distribution.csv"
    )
    birth_place_path = (
        project_root
        / "data"
        / "interim"
        / "dataset_diagnostics_birth_place_distribution.csv"
    )
    duplicate_entities_path = (
        project_root
        / "data"
        / "interim"
        / "dataset_diagnostics_duplicate_entities.csv"
    )
    viaf_conflict_entities_path = (
        project_root
        / "data"
        / "interim"
        / "dataset_diagnostics_viaf_conflict_entities.csv"
    )
    report_path = (
        project_root / "data" / "interim" / "dataset_diagnostics_report.md"
    )
    manifest_path = (
        project_root / "data" / "interim" / "dataset_diagnostics_manifest.csv"
    )

    if not cleaned_path.exists():
        raise FileNotFoundError(
            "Missing cleaned dataset. Run Step 01 and Step 02 first:\n"
            "python scripts/pipeline/01_build_merged_dataset.py\n"
            "python scripts/pipeline/02_clean_structural_fields.py"
        )

    df = pd.read_csv(cleaned_path)
    raw_conflicts_df = (
        pd.read_csv(raw_conflicts_path)
        if raw_conflicts_path.exists()
        else pd.DataFrame(columns=["wikidata_id", "birth_date", "viaf_id"])
    )

    summary_df = build_summary_table(df, raw_conflicts_df)
    missingness_df = build_missingness_table(df)
    occupation_df = build_distribution_table(df, "occupation_raw", "occupation_raw")
    birth_place_df = build_distribution_table(df, "birth_place", "birth_place")
    duplicate_df = build_duplicate_entity_table(df)
    viaf_conflict_df = build_viaf_conflict_entity_table(df, raw_conflicts_df)
    manifest_df = build_manifest_table(
        project_root=project_root,
        output_descriptions=[
            (
                summary_path,
                "High-level row, entity, duplicate, VIAF, date, and label metrics.",
            ),
            (
                missingness_path,
                "Column-by-column missingness and distinct-value counts.",
            ),
            (
                occupation_path,
                "Row and entity distribution by raw occupation label.",
            ),
            (
                birth_place_path,
                "Row and entity distribution by raw birth-place label.",
            ),
            (
                duplicate_entities_path,
                "Entity-level summary of duplicate Wikidata IDs.",
            ),
            (
                viaf_conflict_entities_path,
                "Entity-level summary of ambiguous VIAF dates or identifiers.",
            ),
            (
                report_path,
                "Short Markdown orientation note; CSV files remain canonical.",
            ),
            (
                manifest_path,
                "Machine-readable inventory of diagnostic outputs.",
            ),
        ],
    )
    report_text = build_report(
        summary_df=summary_df,
        manifest_df=manifest_df,
    )

    report_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(summary_path, index=False)
    missingness_df.to_csv(missingness_path, index=False)
    occupation_df.to_csv(occupation_path, index=False)
    birth_place_df.to_csv(birth_place_path, index=False)
    duplicate_df.to_csv(duplicate_entities_path, index=False)
    viaf_conflict_df.to_csv(viaf_conflict_entities_path, index=False)
    manifest_df.to_csv(manifest_path, index=False)
    report_path.write_text(report_text, encoding="utf-8")

    metric_lookup = {
        row.metric: row.value
        for row in summary_df.itertuples(index=False)
    }

    print("Dataset diagnostics complete.")
    print(f"Summary file: {summary_path}")
    print(f"Report file: {report_path}")
    print()
    print(f"Rows: {metric_lookup['total_rows']}")
    print(
        "Distinct entities: "
        f"{metric_lookup['distinct_wikidata_entities']}"
    )
    print(
        "Rows in duplicate entities: "
        f"{metric_lookup['rows_in_duplicate_entities']}"
    )
    print(
        "Surplus duplicate rows: "
        f"{metric_lookup['surplus_duplicate_rows']}"
    )
    print(
        "Rows with ambiguous VIAF metadata: "
        f"{metric_lookup['rows_with_ambiguous_viaf_metadata']}"
    )
    print(
        "Rows with birth year candidate ambiguity: "
        f"{metric_lookup['rows_with_birth_year_candidate_ambiguity']}"
    )
    print(
        "Rows without birth year support from candidates: "
        f"{metric_lookup['rows_without_birth_year_candidate_support']}"
    )
    print(
        "Rows with unresolved name labels: "
        f"{metric_lookup['rows_with_unresolved_name_label']}"
    )


if __name__ == "__main__":
    main()
