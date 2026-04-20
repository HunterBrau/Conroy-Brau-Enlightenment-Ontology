from pathlib import Path
import pandas as pd


def unique_non_null_count(series: pd.Series) -> int:
    values = series.dropna().astype(str)
    return int(values.nunique())


def join_unique_values(series: pd.Series) -> object:
    values = sorted(series.dropna().astype(str).unique())
    if not values:
        return pd.NA
    return " | ".join(values)


def main() -> None:
    # Resolve project root from this script's location.
    project_root = Path(__file__).resolve().parents[2]

    # Define input and output paths.
    geography_path = (
        project_root / "data" / "raw" / "18thcentury_french_writers_table.csv"
    )
    viaf_path = project_root / "data" / "raw" / "18thcentury_writers_wikidata_viaf.csv"
    output_path = project_root / "data" / "interim" / "writers_merged.csv"
    viaf_conflicts_path = project_root / "data" / "interim" / "viaf_conflicts.csv"

    # Read the two raw CSV files.
    geography_df = pd.read_csv(geography_path, header=1)
    viaf_df = pd.read_csv(viaf_path, header=1, dtype={"viaf": "string"})

    # Standardize column names by trimming whitespace.
    geography_df.columns = geography_df.columns.str.strip()
    viaf_df.columns = viaf_df.columns.str.strip()

    # Rename key columns into a more stable shared schema.
    geography_df = geography_df.rename(
        columns={
            "person": "wikidata_id",
            "personLabel": "name",
            "birthYear": "birth_year",
            "birthPlaceLabel": "birth_place",
            "coords": "coords",
            "occupations": "occupation_raw",
        }
    )

    viaf_df = viaf_df.rename(
        columns={
            "person": "wikidata_id",
            "personLabel": "name_viaf",
            "birth": "birth_date",
            "viaf": "viaf_id",
        }
    )

    # Keep only the columns we need from the VIAF table before merging.
    viaf_df = viaf_df[["wikidata_id", "birth_date", "viaf_id"]].copy()

    # Preserve VIAF ambiguity without multiplying geography rows.
    # If a Wikidata entity has multiple VIAF dates or IDs, keep candidates and
    # leave the scalar field blank for later diagnostics rather than choosing one.
    viaf_summary_df = (
        viaf_df.groupby("wikidata_id", dropna=False)
        .agg(
            viaf_record_count=("wikidata_id", "size"),
            viaf_birth_date_count=("birth_date", unique_non_null_count),
            viaf_id_count=("viaf_id", unique_non_null_count),
            birth_date_candidates=("birth_date", join_unique_values),
            viaf_id_candidates=("viaf_id", join_unique_values),
        )
        .reset_index()
    )

    viaf_summary_df["birth_date"] = viaf_summary_df["birth_date_candidates"].where(
        viaf_summary_df["viaf_birth_date_count"] == 1,
        pd.NA,
    )
    viaf_summary_df["viaf_id"] = viaf_summary_df["viaf_id_candidates"].where(
        viaf_summary_df["viaf_id_count"] == 1,
        pd.NA,
    )
    viaf_summary_df["viaf_has_conflict"] = (
        (viaf_summary_df["viaf_birth_date_count"] > 1)
        | (viaf_summary_df["viaf_id_count"] > 1)
    )

    viaf_summary_df = viaf_summary_df[
        [
            "wikidata_id",
            "birth_date",
            "viaf_id",
            "birth_date_candidates",
            "viaf_id_candidates",
            "viaf_record_count",
            "viaf_birth_date_count",
            "viaf_id_count",
            "viaf_has_conflict",
        ]
    ]

    geography_ids = set(geography_df["wikidata_id"].dropna())
    joining_conflict_ids = set(
        viaf_summary_df.loc[
            viaf_summary_df["viaf_has_conflict"]
            & viaf_summary_df["wikidata_id"].isin(geography_ids),
            "wikidata_id",
        ]
    )
    viaf_conflicts_df = viaf_df.loc[
        viaf_df["wikidata_id"].isin(joining_conflict_ids)
    ].sort_values(["wikidata_id", "birth_date", "viaf_id"], na_position="last")

    # Merge: keep every row from the geography table and add VIAF data where available.
    merged_df = geography_df.merge(
        viaf_summary_df,
        on="wikidata_id",
        how="left",
        validate="many_to_one",
    )

    # Ensure output directory exists.
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write merged dataset.
    merged_df.to_csv(output_path, index=False)
    viaf_conflicts_df.to_csv(viaf_conflicts_path, index=False)

    # Lightweight schema preview for terminal inspection.
    print(f"Merged rows: {len(merged_df)}")
    print(f"Columns: {list(merged_df.columns)}")
    print(f"Output written to: {output_path}")
    print(f"VIAF conflict rows written: {len(viaf_conflicts_df)}")
    print(f"VIAF conflicts file: {viaf_conflicts_path}")


if __name__ == "__main__":
    main()
