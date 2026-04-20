from pathlib import Path
import re

import pandas as pd


POINT_PATTERN = re.compile(
    r"Point\(\s*([-+]?\d*\.?\d+)\s+([-+]?\d*\.?\d+)\s*\)"
)


def parse_point(value) -> tuple:
    """
    Parse a Wikidata Point string of the form:
        Point(lon lat)

    Returns:
        (birth_lon, birth_lat)
    """
    if pd.isna(value):
        return (pd.NA, pd.NA)

    text = str(value).strip()
    match = POINT_PATTERN.fullmatch(text)
    if not match:
        return (pd.NA, pd.NA)

    lon = float(match.group(1))
    lat = float(match.group(2))
    return (lon, lat)


def normalize_blank_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert empty strings and whitespace-only strings to pandas NA.
    """
    return df.replace(r"^\s*$", pd.NA, regex=True)


def extract_birth_year_candidates(value) -> set[int]:
    """
    Extract years from a pipe-delimited list of VIAF birth-date candidates.
    """
    if pd.isna(value):
        return set()

    candidates = [part.strip() for part in str(value).split("|") if part.strip()]
    if not candidates:
        return set()

    parsed_dates = pd.to_datetime(candidates, errors="coerce", utc=True)
    return {
        int(year)
        for year in parsed_dates.year
        if not pd.isna(year)
    }


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]

    input_path = project_root / "data" / "interim" / "writers_merged.csv"
    output_path = project_root / "data" / "interim" / "writers_cleaned.csv"
    duplicates_path = project_root / "data" / "interim" / "duplicate_wikidata_ids.csv"

    df = pd.read_csv(input_path)

    # Normalize column names and blank cells.
    df.columns = [str(col).strip() for col in df.columns]
    df = normalize_blank_strings(df)

    # Keep VIAF as string to avoid integer coercion or scientific notation.
    if "viaf_id" in df.columns:
        df["viaf_id"] = df["viaf_id"].astype("string")

    # Standardize wikidata_id and text-like columns.
    text_cols = [
        "wikidata_id",
        "name",
        "birth_place",
        "coords",
        "occupation_raw",
        "birth_date",
        "birth_date_candidates",
        "viaf_id",
        "viaf_id_candidates",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    viaf_count_cols = [
        "viaf_record_count",
        "viaf_birth_date_count",
        "viaf_id_count",
    ]
    for col in viaf_count_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "viaf_has_conflict" in df.columns:
        df["viaf_has_conflict"] = (
            df["viaf_has_conflict"].astype("string").str.lower().eq("true")
        ).fillna(False).astype(bool)

    # Normalize birth_year to nullable integer.
    if "birth_year" in df.columns:
        df["birth_year"] = pd.to_numeric(
            df["birth_year"], errors="coerce"
        ).astype("Int64")

    # Parse full birth_date if present.
    if "birth_date" in df.columns:
        parsed_birth_date = pd.to_datetime(df["birth_date"], errors="coerce", utc=True)
        df["birth_date"] = parsed_birth_date.astype("string")

        # Keep parsed datetime internally for calculations only.
        df["birth_date_parsed"] = parsed_birth_date
        df["birth_year_from_date"] = parsed_birth_date.dt.year.astype("Int64")
    else:
        df["birth_date_parsed"] = pd.NaT
        df["birth_year_from_date"] = pd.Series(
            pd.array([pd.NA] * len(df), dtype="Int64")
        )

    # Parse coordinates into separate numeric fields.
    if "coords" in df.columns:
        parsed_points = df["coords"].apply(parse_point)
        df["birth_lon"] = parsed_points.apply(lambda x: x[0]).astype("Float64")
        df["birth_lat"] = parsed_points.apply(lambda x: x[1]).astype("Float64")
    else:
        df["birth_lon"] = pd.Series(pd.array([pd.NA] * len(df), dtype="Float64"))
        df["birth_lat"] = pd.Series(pd.array([pd.NA] * len(df), dtype="Float64"))

    # Flag unresolved labels that still look like Wikidata QIDs.
    if "name" in df.columns:
        df["name_is_qid"] = df["name"].str.fullmatch(r"Q\d+", na=False)
    else:
        df["name_is_qid"] = False

    if "birth_place" in df.columns:
        df["birth_place_is_qid"] = df["birth_place"].str.fullmatch(r"Q\d+", na=False)
    else:
        df["birth_place_is_qid"] = False

    # Flag duplicate wikidata_ids.
    if "wikidata_id" in df.columns:
        duplicate_mask = df.duplicated(subset=["wikidata_id"], keep=False)
        df["has_duplicate_wikidata_id"] = duplicate_mask

        duplicate_counts = (
            df.groupby("wikidata_id", dropna=False)
            .size()
            .rename("duplicate_row_count")
        )
        df = df.merge(duplicate_counts, on="wikidata_id", how="left")

        duplicates_df = df.loc[duplicate_mask].sort_values(
            by=["wikidata_id", "birth_year", "birth_date"],
            na_position="last",
        )
    else:
        df["has_duplicate_wikidata_id"] = False
        df["duplicate_row_count"] = 1
        duplicates_df = pd.DataFrame()

    # Flag disagreement between birth_year and birth_year_from_date.
    df["birth_year_mismatch"] = (
        df["birth_year"].notna()
        & df["birth_year_from_date"].notna()
        & (df["birth_year"] != df["birth_year_from_date"])
    )

    if "birth_date_candidates" in df.columns:
        birth_year_candidate_sets = df["birth_date_candidates"].apply(
            extract_birth_year_candidates
        )
        df["birth_year_candidate_year_count"] = pd.Series(
            [len(candidate_years) for candidate_years in birth_year_candidate_sets],
            dtype="Int64",
        )
        df["birth_year_candidate_ambiguity_exists"] = [
            len(candidate_years) > 1
            for birth_year, candidate_years in zip(
                df["birth_year"],
                birth_year_candidate_sets,
            )
        ]
        df["birth_year_without_candidate_support"] = [
            pd.notna(birth_year)
            and bool(candidate_years)
            and int(birth_year) not in candidate_years
            for birth_year, candidate_years in zip(
                df["birth_year"],
                birth_year_candidate_sets,
            )
        ]
    else:
        df["birth_year_candidate_year_count"] = pd.Series(
            pd.array([pd.NA] * len(df), dtype="Int64")
        )
        df["birth_year_candidate_ambiguity_exists"] = False
        df["birth_year_without_candidate_support"] = False

    # Reorder columns for easier inspection.
    preferred_order = [
        "wikidata_id",
        "name",
        "name_is_qid",
        "birth_year",
        "birth_year_from_date",
        "birth_year_mismatch",
        "birth_year_candidate_year_count",
        "birth_year_candidate_ambiguity_exists",
        "birth_year_without_candidate_support",
        "birth_date",
        "birth_date_candidates",
        "birth_place",
        "birth_place_is_qid",
        "coords",
        "birth_lon",
        "birth_lat",
        "occupation_raw",
        "viaf_id",
        "viaf_id_candidates",
        "viaf_record_count",
        "viaf_birth_date_count",
        "viaf_id_count",
        "viaf_has_conflict",
        "has_duplicate_wikidata_id",
        "duplicate_row_count",
    ]
    ordered_cols = [col for col in preferred_order if col in df.columns]
    remaining_cols = [
        col for col in df.columns
        if col not in ordered_cols and col != "birth_date_parsed"
    ]
    df = df[ordered_cols + remaining_cols]

    # Build export versions without the internal parsed datetime column.
    drop_cols = ["birth_date_parsed"]
    df_export = df.drop(columns=[c for c in drop_cols if c in df.columns])
    duplicates_export = duplicates_df.drop(
        columns=[c for c in drop_cols if c in duplicates_df.columns]
    )

    # Write outputs.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_export.to_csv(output_path, index=False)
    duplicates_export.to_csv(duplicates_path, index=False)

    # Terminal summary.
    print(f"Rows written: {len(df_export)}")
    print(f"Cleaned dataset: {output_path}")
    print(f"Duplicate rows file: {duplicates_path}")
    print()

    if "has_duplicate_wikidata_id" in df_export.columns:
        print(
            f"Rows with duplicate wikidata_id: "
            f"{int(df_export['has_duplicate_wikidata_id'].sum())}"
        )

    if "birth_year_mismatch" in df_export.columns:
        print(
            f"Rows with birth year mismatch: "
            f"{int(df_export['birth_year_mismatch'].sum())}"
        )

    if "birth_year_candidate_ambiguity_exists" in df_export.columns:
        print(
            f"Rows with birth year candidate ambiguity: "
            f"{int(df_export['birth_year_candidate_ambiguity_exists'].sum())}"
        )

    if "birth_year_without_candidate_support" in df_export.columns:
        print(
            f"Rows without birth year support from candidates: "
            f"{int(df_export['birth_year_without_candidate_support'].sum())}"
        )

    if "name_is_qid" in df_export.columns:
        print(
            f"Rows with unresolved name labels: "
            f"{int(df_export['name_is_qid'].sum())}"
        )

    if "birth_place_is_qid" in df_export.columns:
        print(
            f"Rows with unresolved birth place labels: "
            f"{int(df_export['birth_place_is_qid'].sum())}"
        )

    if "viaf_id" in df_export.columns:
        print(f"Rows with VIAF ID present: {int(df_export['viaf_id'].notna().sum())}")

    if "viaf_has_conflict" in df_export.columns:
        print(
            f"Rows with ambiguous VIAF metadata: "
            f"{int(df_export['viaf_has_conflict'].sum())}"
        )


if __name__ == "__main__":
    main()
