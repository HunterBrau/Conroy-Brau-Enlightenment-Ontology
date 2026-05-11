from pathlib import Path
from argparse import ArgumentParser
import sys
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import COHORT_IDS, DEFAULT_COHORT_ID, cohort_paths  # noqa: E402


def unique_non_null_count(series: pd.Series) -> int:
    values = series.dropna().astype(str)
    return int(values.nunique())


def join_unique_values(series: pd.Series) -> object:
    values = sorted(series.dropna().astype(str).unique())
    if not values:
        return pd.NA
    return " | ".join(values)


def parse_args() -> object:
    parser = ArgumentParser(description="Build a merged cohort dataset.")
    parser.add_argument(
        "--cohort-id",
        default=DEFAULT_COHORT_ID,
        choices=COHORT_IDS,
        help=f"Cohort to build. Default: {DEFAULT_COHORT_ID}.",
    )
    return parser.parse_args()


def normalize_french_seed_discovery(discovery_path: Path) -> pd.DataFrame:
    geography_df = pd.read_csv(discovery_path, header=1)
    geography_df.columns = geography_df.columns.str.strip()
    return geography_df.rename(
        columns={
            "person": "wikidata_id",
            "personLabel": "name",
            "birthYear": "birth_year",
            "birthPlaceLabel": "birth_place",
            "coords": "coords",
            "occupations": "occupation_raw",
        }
    )


def normalize_global_discovery(discovery_path: Path) -> pd.DataFrame:
    discovery_df = pd.read_csv(discovery_path)
    discovery_df.columns = discovery_df.columns.str.strip()
    if "person" not in discovery_df.columns:
        raise SystemExit(f"Global discovery file must contain a person column: {discovery_path}")

    discovery_df = discovery_df.rename(
        columns={
            "person": "wikidata_id",
            "personLabel": "name",
            "birthDate": "birth_date",
            "birthYear": "birth_year",
            "birthPlaceLabel": "birth_place",
            "occupation_labels": "occupation_raw",
        }
    )

    for column in ["birth_place", "coords", "occupation_raw"]:
        if column not in discovery_df.columns:
            discovery_df[column] = pd.NA
    if "occupation_raw" in discovery_df.columns:
        discovery_df["occupation_raw"] = discovery_df["occupation_raw"].fillna(
            discovery_df.get("occupation_ids", pd.Series(pd.NA, index=discovery_df.index))
        )

    keep_columns = [
        "wikidata_id",
        "name",
        "birth_year",
        "birth_place",
        "coords",
        "occupation_raw",
        "birth_date",
    ]
    return discovery_df[[column for column in keep_columns if column in discovery_df.columns]].copy()


def summarize_viaf(viaf_path: Path, geography_ids: set[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    viaf_df = pd.read_csv(viaf_path, header=1, dtype={"viaf": "string"})
    viaf_df.columns = viaf_df.columns.str.strip()

    viaf_df = viaf_df.rename(
        columns={
            "person": "wikidata_id",
            "personLabel": "name_viaf",
            "birth": "birth_date",
            "viaf": "viaf_id",
        }
    )

    viaf_df = viaf_df[["wikidata_id", "birth_date", "viaf_id"]].copy()

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

    return viaf_summary_df, viaf_conflicts_df


def empty_viaf_summary(geography_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    viaf_summary_df = geography_df[["wikidata_id"]].drop_duplicates().copy()
    viaf_summary_df["birth_date_candidates"] = geography_df.get("birth_date", pd.NA)
    viaf_summary_df["viaf_id_candidates"] = pd.NA
    viaf_summary_df["viaf_record_count"] = 0
    viaf_summary_df["viaf_birth_date_count"] = 0
    viaf_summary_df["viaf_id_count"] = 0
    viaf_summary_df["viaf_has_conflict"] = False
    viaf_summary_df["viaf_id"] = pd.NA
    if "birth_date" in geography_df.columns:
        viaf_summary_df = viaf_summary_df.drop(columns=["birth_date_candidates"]).merge(
            geography_df[["wikidata_id", "birth_date"]].drop_duplicates("wikidata_id"),
            on="wikidata_id",
            how="left",
        )
        viaf_summary_df["birth_date_candidates"] = viaf_summary_df["birth_date"]
    else:
        viaf_summary_df["birth_date"] = pd.NA
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
    return viaf_summary_df, pd.DataFrame(columns=["wikidata_id", "birth_date", "viaf_id"])


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, args.cohort_id)

    if not paths.raw_discovery_path.exists():
        raise SystemExit(f"Missing discovery file for {paths.cohort_id}: {paths.raw_discovery_path}")

    if paths.cohort_id == "french_seed":
        geography_df = normalize_french_seed_discovery(paths.raw_discovery_path)
    else:
        geography_df = normalize_global_discovery(paths.raw_discovery_path)

    geography_ids = set(geography_df["wikidata_id"].dropna())
    if paths.raw_viaf_path and paths.raw_viaf_path.exists():
        viaf_summary_df, viaf_conflicts_df = summarize_viaf(paths.raw_viaf_path, geography_ids)
    else:
        viaf_summary_df, viaf_conflicts_df = empty_viaf_summary(geography_df)

    merge_base = geography_df.drop(columns=["birth_date"], errors="ignore")
    merged_df = merge_base.merge(
        viaf_summary_df,
        on="wikidata_id",
        how="left",
        validate="many_to_one",
    )

    paths.merged_path.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(paths.merged_path, index=False)
    viaf_conflicts_df.to_csv(paths.viaf_conflicts_path, index=False)

    print(f"Cohort: {paths.cohort_id}")
    print(f"Merged rows: {len(merged_df)}")
    print(f"Columns: {list(merged_df.columns)}")
    print(f"Output written to: {paths.merged_path}")
    print(f"VIAF conflict rows written: {len(viaf_conflicts_df)}")
    print(f"VIAF conflicts file: {paths.viaf_conflicts_path}")


if __name__ == "__main__":
    main()
