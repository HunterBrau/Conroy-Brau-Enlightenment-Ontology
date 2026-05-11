"""
Build reproducible context slices from the global writer cohort.

This layer replaces the manual French seed as a discovery source for new
comparative claims. It derives France, Germany, British, and China/Qing slices
from the same global writer/subclass cohort and the reviewed political-entity
crosswalk, then writes an audit showing how the legacy French seed overlaps
with those reproducible slices.
"""

from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import COHORT_IDS, cohort_paths  # noqa: E402
from common import join_values, percentage, qid_from_uri, split_pipe_values  # noqa: E402
from crosswalk import load_political_crosswalk  # noqa: E402


PLACE_TOKEN_COLUMNS = [
    "place_id",
    "direct_country_ids",
    "admin_country_ids",
    "context_country_ids",
    "admin_entity_ids",
]


def normalize_slice_id(review_group: str) -> str:
    return review_group.strip().lower().replace("/", "_").replace(" ", "_")


def labels_for(ids: set[str], label_lookup: dict[str, str]) -> object:
    return join_values([label_lookup.get(qid, qid) for qid in ids], sort=True)


def load_entity_table(enriched_path: Path) -> pd.DataFrame:
    usecols = [
        "wikidata_id",
        "name",
        "birth_year",
        "citizenship_ids",
        "citizenship_labels",
    ]
    df = pd.read_csv(enriched_path, usecols=usecols).drop_duplicates("wikidata_id")
    df["person_id"] = df["wikidata_id"].apply(qid_from_uri)
    return df.sort_values(["name", "wikidata_id"])


def load_legacy_french_seed_qids(project_root: Path) -> set[str]:
    raw_path = project_root / "data" / "raw" / "18thcentury_french_writers_table.csv"
    if not raw_path.exists():
        return set()

    raw_df = pd.read_csv(raw_path, header=1, usecols=["person"])
    return set(raw_df["person"].apply(qid_from_uri).dropna())


def group_matches(tokens: set[str], group_lookup: dict[str, str]) -> dict[str, set[str]]:
    matches: dict[str, set[str]] = defaultdict(set)
    for token in tokens:
        review_group = group_lookup.get(token)
        if review_group:
            matches[review_group].add(token)
    return matches


def build_citizenship_matches(
    entity_df: pd.DataFrame,
    group_lookup: dict[str, str],
) -> dict[str, dict[str, set[str]]]:
    matches: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in entity_df.itertuples(index=False):
        token_ids = set(split_pipe_values(row.citizenship_ids))
        for review_group, ids in group_matches(token_ids, group_lookup).items():
            matches[row.wikidata_id][review_group].update(ids)
    return matches


def row_place_tokens(row) -> set[str]:
    tokens = set()
    for column in PLACE_TOKEN_COLUMNS:
        if not hasattr(row, column):
            continue
        values = split_pipe_values(getattr(row, column))
        if column == "place_id":
            values = [qid_from_uri(value) for value in values]
        tokens.update(str(value) for value in values if pd.notna(value))
    return tokens


def build_place_matches(
    place_context_path: Path,
    group_lookup: dict[str, str],
) -> dict[str, dict[str, dict[str, set[str]]]]:
    matches: dict[str, dict[str, dict[str, set[str]]]] = defaultdict(
        lambda: defaultdict(lambda: {"ids": set(), "birth_ids": set(), "roles": set()})
    )
    if not place_context_path.exists():
        return matches

    place_df = pd.read_csv(place_context_path)
    for row in place_df.itertuples(index=False):
        token_ids = row_place_tokens(row)
        for review_group, ids in group_matches(token_ids, group_lookup).items():
            group_match = matches[row.wikidata_id][review_group]
            group_match["ids"].update(ids)
            group_match["roles"].add(row.place_role)
            if row.place_role == "birth_place":
                group_match["birth_ids"].update(ids)
    return matches


def build_membership_table(
    entity_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    legacy_seed_qids: set[str],
    citizenship_matches: dict[str, dict[str, set[str]]],
    place_matches: dict[str, dict[str, dict[str, set[str]]]],
) -> pd.DataFrame:
    label_lookup = dict(zip(crosswalk_df["wikidata_id"], crosswalk_df["label"], strict=False))
    rows = []

    for entity in entity_df.itertuples(index=False):
        groups = set(citizenship_matches.get(entity.wikidata_id, {})) | set(place_matches.get(entity.wikidata_id, {}))
        for review_group in sorted(groups):
            citizenship_ids = citizenship_matches.get(entity.wikidata_id, {}).get(review_group, set())
            place_group = place_matches.get(entity.wikidata_id, {}).get(
                review_group,
                {"ids": set(), "birth_ids": set(), "roles": set()},
            )
            place_ids = place_group["ids"]
            birth_ids = place_group["birth_ids"]
            roles = place_group["roles"]
            rows.append(
                {
                    "wikidata_id": entity.wikidata_id,
                    "person_id": entity.person_id,
                    "name": entity.name,
                    "birth_year": entity.birth_year,
                    "slice_id": normalize_slice_id(review_group),
                    "review_group": review_group,
                    "has_any_slice_evidence": True,
                    "has_citizenship_evidence": bool(citizenship_ids),
                    "has_place_context_evidence": bool(place_ids),
                    "has_birth_place_context_evidence": bool(birth_ids),
                    "citizenship_match_ids": join_values(citizenship_ids, sort=True),
                    "citizenship_match_labels": labels_for(citizenship_ids, label_lookup),
                    "place_context_match_ids": join_values(place_ids, sort=True),
                    "place_context_match_labels": labels_for(place_ids, label_lookup),
                    "birth_place_context_match_ids": join_values(birth_ids, sort=True),
                    "birth_place_context_match_labels": labels_for(birth_ids, label_lookup),
                    "matched_place_roles": join_values(roles, sort=True),
                    "legacy_french_seed_member": entity.person_id in legacy_seed_qids,
                }
            )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values(["slice_id", "name", "person_id"])


def build_summary_table(membership_df: pd.DataFrame, total_entities: int, legacy_seed_qids: set[str]) -> pd.DataFrame:
    if membership_df.empty:
        return pd.DataFrame()

    rows = []
    for review_group, group in membership_df.groupby("review_group", sort=True):
        has_citizenship = group["has_citizenship_evidence"]
        has_place = group["has_place_context_evidence"]
        entity_count = group["person_id"].nunique()
        legacy_overlap = group.loc[group["legacy_french_seed_member"], "person_id"].nunique()
        rows.append(
            {
                "slice_id": normalize_slice_id(review_group),
                "review_group": review_group,
                "global_entities": total_entities,
                "entities_with_any_slice_evidence": entity_count,
                "entities_with_any_slice_evidence_pct": percentage(entity_count, total_entities),
                "entities_with_citizenship_evidence": group.loc[has_citizenship, "person_id"].nunique(),
                "entities_with_place_context_evidence": group.loc[has_place, "person_id"].nunique(),
                "entities_with_birth_place_context_evidence": group.loc[
                    group["has_birth_place_context_evidence"],
                    "person_id",
                ].nunique(),
                "entities_with_citizenship_and_place_context": group.loc[
                    has_citizenship & has_place,
                    "person_id",
                ].nunique(),
                "entities_with_citizenship_only": group.loc[has_citizenship & ~has_place, "person_id"].nunique(),
                "entities_with_place_context_only": group.loc[~has_citizenship & has_place, "person_id"].nunique(),
                "legacy_french_seed_overlap": legacy_overlap,
                "legacy_french_seed_overlap_pct": percentage(legacy_overlap, len(legacy_seed_qids)),
            }
        )

    return pd.DataFrame(rows).sort_values("slice_id")


def qids_with_tokens(df: pd.DataFrame, token_ids: set[str]) -> set[str]:
    matched = set()
    for row in df.itertuples(index=False):
        if set(split_pipe_values(row.citizenship_ids)) & token_ids:
            matched.add(row.person_id)
    return matched


def build_legacy_audit(
    project_root: Path,
    entity_df: pd.DataFrame,
    membership_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    legacy_seed_qids: set[str],
) -> pd.DataFrame:
    legacy_raw_path = project_root / "data" / "raw" / "18thcentury_french_writers_table.csv"
    legacy_raw_rows = len(pd.read_csv(legacy_raw_path, header=1)) if legacy_raw_path.exists() else 0
    global_qids = set(entity_df["person_id"].dropna())
    exact_france_qids = qids_with_tokens(entity_df, {"Q142"})
    france_crosswalk_ids = set(crosswalk_df.loc[crosswalk_df["review_group"] == "France", "wikidata_id"])
    global_france_citizenship_qids = qids_with_tokens(entity_df, france_crosswalk_ids)

    legacy_enriched_path = project_root / "data" / "interim" / "writers_wikidata_enriched.csv"
    if legacy_enriched_path.exists():
        legacy_entity_df = load_entity_table(legacy_enriched_path)
        legacy_exact_france_qids = qids_with_tokens(legacy_entity_df, {"Q142"})
        legacy_france_citizenship_qids = qids_with_tokens(legacy_entity_df, france_crosswalk_ids)
    else:
        legacy_exact_france_qids = set()
        legacy_france_citizenship_qids = set()

    france_slice_qids = set(
        membership_df.loc[membership_df["review_group"] == "France", "person_id"]
    ) if not membership_df.empty else set()

    rows = [
        ("legacy_seed_raw_rows", legacy_raw_rows, "Rows in the original manual French-seed CSV."),
        ("legacy_seed_distinct_qids", len(legacy_seed_qids), "Distinct QIDs in the original manual French seed."),
        (
            "legacy_seed_qids_in_global_writers",
            len(legacy_seed_qids & global_qids),
            "Legacy seed QIDs already present in the reproducible global writer cohort.",
        ),
        (
            "legacy_seed_qids_missing_from_global_writers",
            len(legacy_seed_qids - global_qids),
            "Legacy seed QIDs not present in the reproducible global writer cohort.",
        ),
        ("global_exact_france_q142_citizenship", len(exact_france_qids), "Global writers with P27 exactly France/Q142."),
        (
            "global_crosswalk_france_citizenship",
            len(global_france_citizenship_qids),
            "Global writers with citizenship matching any France review-group QID in the crosswalk.",
        ),
        (
            "global_france_any_citizenship_or_place_context",
            len(france_slice_qids),
            "Global writers with France crosswalk evidence in citizenship or place context.",
        ),
        (
            "legacy_seed_exact_france_q142_citizenship",
            len(legacy_exact_france_qids),
            "Legacy seed QIDs with current P27 exactly France/Q142.",
        ),
        (
            "legacy_seed_crosswalk_france_citizenship",
            len(legacy_france_citizenship_qids),
            "Legacy seed QIDs with current citizenship matching any France review-group QID.",
        ),
        (
            "global_exact_q142_not_in_legacy_seed",
            len(exact_france_qids - legacy_seed_qids),
            "Exact France/Q142 global writers absent from the legacy manual seed.",
        ),
        (
            "legacy_seed_not_exact_q142",
            len(legacy_seed_qids - exact_france_qids),
            "Legacy seed QIDs that are not current exact France/Q142 citizens in the global cohort.",
        ),
        (
            "global_crosswalk_france_citizenship_not_in_legacy_seed",
            len(global_france_citizenship_qids - legacy_seed_qids),
            "Crosswalk-France citizenship global writers absent from the legacy manual seed.",
        ),
        (
            "legacy_seed_not_crosswalk_france_citizenship",
            len(legacy_seed_qids - global_france_citizenship_qids),
            "Legacy seed QIDs without current crosswalk-France citizenship in the global cohort.",
        ),
    ]
    return pd.DataFrame(rows, columns=["metric", "value", "notes"])


def parse_args() -> object:
    parser = ArgumentParser(description="Build reproducible global context slices from the political crosswalk.")
    parser.add_argument(
        "--cohort-id",
        default="global_writers",
        choices=COHORT_IDS,
        help="Cohort to slice. Default: global_writers.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, args.cohort_id)
    crosswalk_df = load_political_crosswalk(project_root)
    if crosswalk_df.empty:
        raise SystemExit("Missing political entity crosswalk seed.")
    if not paths.enriched_path.exists():
        raise SystemExit(f"Missing enriched cohort: {paths.enriched_path}")
    if not (paths.processed_dir / "place_context_long.csv").exists():
        raise SystemExit(f"Missing place context: {paths.processed_dir / 'place_context_long.csv'}")

    crosswalk_df = crosswalk_df.loc[crosswalk_df["review_group"].notna()].copy()
    group_lookup = dict(zip(crosswalk_df["wikidata_id"], crosswalk_df["review_group"], strict=False))
    legacy_seed_qids = load_legacy_french_seed_qids(project_root)
    entity_df = load_entity_table(paths.enriched_path)
    citizenship_matches = build_citizenship_matches(entity_df, group_lookup)
    place_matches = build_place_matches(paths.processed_dir / "place_context_long.csv", group_lookup)
    membership_df = build_membership_table(
        entity_df,
        crosswalk_df,
        legacy_seed_qids,
        citizenship_matches,
        place_matches,
    )
    summary_df = build_summary_table(membership_df, len(entity_df), legacy_seed_qids)

    paths.processed_dir.mkdir(parents=True, exist_ok=True)
    membership_path = paths.processed_dir / "context_slice_membership.csv"
    summary_path = paths.processed_dir / "context_slice_summary.csv"
    membership_df.to_csv(membership_path, index=False)
    summary_df.to_csv(summary_path, index=False)

    audit_path = None
    if args.cohort_id == "global_writers":
        audit_df = build_legacy_audit(project_root, entity_df, membership_df, crosswalk_df, legacy_seed_qids)
        audit_path = project_root / "data" / "processed" / "french_seed_redundancy_audit.csv"
        audit_df.to_csv(audit_path, index=False)

    print("Context slice tables complete.")
    print(f"Cohort: {paths.cohort_id}")
    print(f"Entity rows: {len(entity_df)}")
    print(f"Slice membership rows: {len(membership_df)}")
    print(f"Summary rows: {len(summary_df)}")
    print(f"Membership: {membership_path}")
    print(f"Summary: {summary_path}")
    if audit_path:
        print(f"Legacy French seed audit: {audit_path}")
    if not summary_df.empty:
        print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
