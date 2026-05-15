"""
Build a conference-facing insight mining packet from existing global data.

This layer does not fetch data or introduce new sources. It mines the current
`global_writers` processed tables for claim candidates, second-layer insight
tables, reproducible example entities, and a gate on whether Wikipedia article
metadata should be collected later.
"""

from pathlib import Path
import re
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import DEFAULT_COHORT_ID, cohort_paths  # noqa: E402
from common import WIKI_COLUMNS, bool_value, percentage  # noqa: E402


CONTEXT_ORDER = ["france", "germany", "british", "china"]
CONTEXT_LABELS = {
    "france": "France",
    "germany": "Germany",
    "british": "British",
    "china": "China/Qing",
}
GLOBAL_SCOPE_ID = "global_writers"
GLOBAL_SCOPE_LABEL = "Global writers"
ALL_GENDERS = "All genders"
ALL_BUCKETS = "All occupation buckets"

OUTPUT_FILENAMES = {
    "claims": "insight_claim_candidates.csv",
    "gender_context": "insight_gender_context.csv",
    "gender_language": "insight_gender_language_representation.csv",
    "occupation_overrepresentation": "insight_occupation_overrepresentation.csv",
    "decade_trends": "insight_decade_trends.csv",
    "multi_context": "insight_multi_context_entities.csv",
    "data_friction": "insight_data_friction_by_context_gender_bucket.csv",
    "examples": "insight_example_entities.csv",
    "metadata_gate": "insight_metadata_gap_assessment.csv",
}

FRICTION_METRICS = [
    (
        "missing_country_of_citizenship",
        "Missing citizenship",
        "Entity has no Wikidata country-of-citizenship value.",
    ),
    (
        "no_place_context",
        "No place context",
        "Entity has no mapped birth/death/residence/work-location evidence.",
    ),
    (
        "no_tracked_wikipedia_article",
        "No tracked Wikipedia article",
        "Entity has no article in the tracked language-edition matrix.",
    ),
    (
        "low_evidence_affiliation",
        "Low-evidence affiliation",
        "Top affiliation candidate is supported by zero or one mapped evidence field.",
    ),
    (
        "no_mapped_affiliation_candidate",
        "No mapped affiliation candidate",
        "Entity has no mapped candidate in the affiliation evidence table.",
    ),
    (
        "top_affiliation_tie",
        "Top affiliation tie",
        "Entity has more than one top-ranked affiliation candidate.",
    ),
]

CLAIM_OCCUPATION_BUCKETS = [
    "Visual Arts / Architecture / Design",
    "Print / Publishing / Journalism",
    "Education / Scholarship / Humanities",
    "Religion / Theology",
    "Politics / Statecraft / Diplomacy",
    "Law / Administration",
    "Philosophy",
    "Science / Natural History",
    "Translation / Philology / Languages",
]


def context_label(slice_id: str) -> str:
    return CONTEXT_LABELS.get(slice_id, slice_id)


def context_order(slice_id: str) -> int:
    if slice_id == GLOBAL_SCOPE_ID:
        return -1
    return CONTEXT_ORDER.index(slice_id) if slice_id in CONTEXT_ORDER else 99


def read_processed(paths) -> dict[str, pd.DataFrame]:
    processed_dir = paths.processed_dir
    filenames = {
        "entities": "representation_entities.csv",
        "membership": "context_slice_membership.csv",
        "gender_by_slice": "core_findings_gender_by_slice.csv",
        "language_by_slice": "core_findings_language_by_slice.csv",
        "occupation_by_slice": "core_findings_occupation_buckets_by_slice.csv",
        "occupation_summary": "occupation_bucket_summary.csv",
        "occupation_entities": "occupation_bucket_entities_long.csv",
        "friction": "core_findings_data_friction.csv",
        "context": "core_findings_context_slices.csv",
        "affiliation_best": "cultural_affiliation_evidence_best.csv",
    }
    tables = {}
    for key, filename in filenames.items():
        path = processed_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Required input is missing: {path}")
        tables[key] = pd.read_csv(path)
    return tables


def first_birth_year(value) -> object:
    if pd.isna(value):
        return pd.NA
    match = re.search(r"\d{4}", str(value))
    if not match:
        return pd.NA
    return int(match.group(0))


def repair_mojibake(value):
    if pd.isna(value):
        return value
    text = str(value)
    if not any(marker in text for marker in ["Ã", "Â", "Å"]):
        return text
    try:
        return text.encode("latin1").decode("utf-8")
    except UnicodeError:
        return text


def clean_entities(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    entities = tables["entities"].copy()
    affiliation = tables["affiliation_best"][
        [
            "wikidata_id",
            "candidate_affiliation_count",
            "top_supporting_evidence_count",
            "top_candidate_tie_count",
            "best_candidate_affiliation",
            "top_candidate_affiliations",
        ]
    ].copy()
    entities = entities.merge(affiliation, on="wikidata_id", how="left")
    for column in ["name", "occupation_labels"]:
        if column in entities:
            entities[column] = entities[column].apply(repair_mojibake)
    entities["birth_year"] = entities["birth_year_values"].apply(first_birth_year)
    entities["birth_decade"] = (
        pd.to_numeric(entities["birth_year"], errors="coerce").floordiv(10).mul(10)
    )
    entities["birth_decade"] = entities["birth_decade"].astype("Int64")
    entities["gender_category"] = entities["gender_category"].fillna("unknown")
    entities["citizenship_ids"] = entities["citizenship_ids"].fillna("")
    entities["wikipedia_sitelink_count"] = (
        pd.to_numeric(entities["wikipedia_sitelink_count"], errors="coerce").fillna(0).astype(int)
    )
    entities["place_evidence_role_count"] = (
        pd.to_numeric(entities["place_evidence_role_count"], errors="coerce").fillna(0).astype(int)
    )
    entities["candidate_affiliation_count"] = (
        pd.to_numeric(entities["candidate_affiliation_count"], errors="coerce").fillna(0).astype(int)
    )
    entities["top_supporting_evidence_count"] = (
        pd.to_numeric(entities["top_supporting_evidence_count"], errors="coerce").fillna(0).astype(int)
    )
    entities["top_candidate_tie_count"] = (
        pd.to_numeric(entities["top_candidate_tie_count"], errors="coerce").fillna(0).astype(int)
    )
    entities["missing_country_of_citizenship"] = entities["citizenship_ids"].str.strip().eq("")
    entities["has_any_tracked_wikipedia_article"] = entities["wikipedia_sitelink_count"].gt(0)
    entities["no_tracked_wikipedia_article"] = ~entities["has_any_tracked_wikipedia_article"]
    entities["no_place_context"] = entities["place_evidence_role_count"].eq(0)
    entities["low_evidence_affiliation"] = entities["top_supporting_evidence_count"].le(1)
    entities["no_mapped_affiliation_candidate"] = entities["candidate_affiliation_count"].eq(0)
    entities["top_affiliation_tie"] = entities["top_candidate_tie_count"].gt(1)
    for column in WIKI_COLUMNS.values():
        if column in entities:
            entities[column] = entities[column].apply(bool_value)
    return entities


def membership_with_labels(membership: pd.DataFrame) -> pd.DataFrame:
    output = membership[membership["slice_id"].isin(CONTEXT_ORDER)].copy()
    output["context_label"] = output["slice_id"].map(context_label)
    output["context_order"] = output["slice_id"].map(context_order)
    return output


def make_gender_context(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    gender = tables["gender_by_slice"].copy()
    gender = gender[gender["slice_id"].isin(CONTEXT_ORDER)].copy()
    gender["context_label"] = gender["slice_id"].map(context_label)
    gender["context_order"] = gender["slice_id"].map(context_order)
    gender["representation_index_vs_global"] = gender.apply(
        lambda row: round(row["slice_pct"] / row["global_pct"], 4)
        if row["global_pct"]
        else pd.NA,
        axis=1,
    )
    gender["notes"] = "Context slices are non-exclusive; pct is within each context slice."
    return gender.sort_values(["context_order", "gender_category"])


def make_gender_language_representation(tables: dict[str, pd.DataFrame], entities: pd.DataFrame) -> pd.DataFrame:
    membership = membership_with_labels(tables["membership"])
    entity_columns = ["wikidata_id", "gender_category", *WIKI_COLUMNS.values()]
    slice_entities = membership[["wikidata_id", "slice_id", "context_label", "context_order"]].merge(
        entities[entity_columns],
        on="wikidata_id",
        how="left",
    )
    rows = []
    global_gender_totals = entities.groupby("gender_category")["wikidata_id"].nunique().to_dict()

    for slice_id, slice_group in slice_entities.groupby("slice_id", sort=False):
        for gender_category, gender_group in slice_group.groupby("gender_category", sort=True):
            denominator = gender_group["wikidata_id"].nunique()
            for language_edition, column in WIKI_COLUMNS.items():
                represented_ids = gender_group.loc[gender_group[column], "wikidata_id"].drop_duplicates()
                represented = len(represented_ids)
                global_gender = entities.loc[entities["gender_category"] == gender_category]
                global_denominator = global_gender_totals.get(gender_category, 0)
                global_represented = int(global_gender[column].sum()) if column in global_gender else 0
                pct = percentage(represented, denominator)
                global_pct = percentage(global_represented, global_denominator)
                rows.append(
                    {
                        "slice_id": slice_id,
                        "context_label": context_label(slice_id),
                        "context_order": context_order(slice_id),
                        "gender_category": gender_category,
                        "language_edition": language_edition,
                        "slice_gender_entities": denominator,
                        "represented_entities": represented,
                        "representation_pct": pct,
                        "global_gender_entities": global_denominator,
                        "global_gender_representation_pct": global_pct,
                        "representation_index_vs_global_gender": round(pct / global_pct, 4)
                        if global_pct
                        else pd.NA,
                        "notes": "Article presence in tracked editions; not article depth or cultural importance.",
                    }
                )
    return pd.DataFrame(rows).sort_values(["context_order", "gender_category", "language_edition"])


def make_occupation_overrepresentation(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    by_slice = tables["occupation_by_slice"].copy()
    global_summary = tables["occupation_summary"][
        ["granular_bucket", "entity_count", "entity_pct"]
    ].rename(columns={"entity_count": "global_entities", "entity_pct": "global_pct"})
    output = by_slice.merge(global_summary, on="granular_bucket", how="left")
    output = output[output["slice_id"].isin(CONTEXT_ORDER)].copy()
    output["context_label"] = output["slice_id"].map(context_label)
    output["context_order"] = output["slice_id"].map(context_order)
    output["pct_point_difference_vs_global"] = (output["slice_pct"] - output["global_pct"]).round(2)
    output["index_vs_global"] = output.apply(
        lambda row: round(row["slice_pct"] / row["global_pct"], 4)
        if row["global_pct"]
        else pd.NA,
        axis=1,
    )

    def signal_class(row) -> str:
        if row["slice_entities"] < 10 or row["global_pct"] < 0.25:
            return "small_n"
        if row["index_vs_global"] >= 1.5:
            return "strong_overrepresented"
        if row["index_vs_global"] >= 1.25:
            return "overrepresented"
        if row["index_vs_global"] <= 0.5:
            return "strong_underrepresented"
        if row["index_vs_global"] <= 0.75:
            return "underrepresented"
        return "near_baseline"

    output["signal_class"] = output.apply(signal_class, axis=1)
    output["notes"] = "Index compares within-slice bucket share to global cohort bucket share."
    return output.sort_values(["context_order", "index_vs_global"], ascending=[True, False])


def scoped_entity_sets(membership: pd.DataFrame, entities: pd.DataFrame) -> dict[str, pd.Index]:
    scopes = {GLOBAL_SCOPE_ID: entities["wikidata_id"].drop_duplicates()}
    for slice_id, group in membership.groupby("slice_id", sort=False):
        if slice_id in CONTEXT_ORDER:
            scopes[slice_id] = group["wikidata_id"].drop_duplicates()
    return scopes


def make_decade_trends(tables: dict[str, pd.DataFrame], entities: pd.DataFrame) -> pd.DataFrame:
    membership = membership_with_labels(tables["membership"])
    scopes = scoped_entity_sets(membership, entities)
    indexed = entities.set_index("wikidata_id")
    rows = []
    for scope_id, ids in scopes.items():
        scope_df = indexed.loc[indexed.index.intersection(ids)].copy()
        scope_df = scope_df[scope_df["birth_decade"].notna()].copy()
        for decade, group in scope_df.groupby("birth_decade", sort=True):
            total = len(group)
            female = int((group["gender_category"] == "female").sum())
            male = int((group["gender_category"] == "male").sum())
            missing_citizenship = int(group["missing_country_of_citizenship"].sum())
            has_wiki = int(group["has_any_tracked_wikipedia_article"].sum())
            no_place = int(group["no_place_context"].sum())
            low_evidence = int(group["low_evidence_affiliation"].sum())
            rows.append(
                {
                    "scope_id": scope_id,
                    "scope_label": GLOBAL_SCOPE_LABEL if scope_id == GLOBAL_SCOPE_ID else context_label(scope_id),
                    "context_order": context_order(scope_id),
                    "birth_decade": int(decade),
                    "entities": total,
                    "female_entities": female,
                    "female_pct": percentage(female, total),
                    "male_entities": male,
                    "male_pct": percentage(male, total),
                    "missing_citizenship_entities": missing_citizenship,
                    "missing_citizenship_pct": percentage(missing_citizenship, total),
                    "wikipedia_article_entities": has_wiki,
                    "wikipedia_article_pct": percentage(has_wiki, total),
                    "no_place_context_entities": no_place,
                    "no_place_context_pct": percentage(no_place, total),
                    "low_evidence_affiliation_entities": low_evidence,
                    "low_evidence_affiliation_pct": percentage(low_evidence, total),
                    "notes": "Birth decades are derived from Wikidata birth-year values; the 1670s row covers 1675-1679.",
                }
            )
    return pd.DataFrame(rows).sort_values(["context_order", "birth_decade"])


def join_context_lists(membership: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for wikidata_id, group in membership.groupby("wikidata_id", sort=False):
        slice_ids = sorted(group["slice_id"].drop_duplicates(), key=context_order)
        rows.append(
            {
                "wikidata_id": wikidata_id,
                "context_count": len(slice_ids),
                "context_slice_ids": " | ".join(slice_ids),
                "context_labels": " | ".join(context_label(slice_id) for slice_id in slice_ids),
                "citizenship_contexts": " | ".join(
                    context_label(row.slice_id)
                    for row in group.itertuples(index=False)
                    if bool_value(row.has_citizenship_evidence)
                ),
                "place_contexts": " | ".join(
                    context_label(row.slice_id)
                    for row in group.itertuples(index=False)
                    if bool_value(row.has_place_context_evidence)
                ),
                "birth_place_contexts": " | ".join(
                    context_label(row.slice_id)
                    for row in group.itertuples(index=False)
                    if bool_value(row.has_birth_place_context_evidence)
                ),
            }
        )
    return pd.DataFrame(rows)


def make_multi_context_entities(tables: dict[str, pd.DataFrame], entities: pd.DataFrame) -> pd.DataFrame:
    membership = membership_with_labels(tables["membership"])
    context_lists = join_context_lists(membership)
    output = context_lists.loc[context_lists["context_count"] > 1].merge(
        entities[
            [
                "wikidata_id",
                "name",
                "birth_year",
                "gender_category",
                "occupation_labels",
                "wikipedia_sitelink_count",
                "missing_country_of_citizenship",
                "low_evidence_affiliation",
            ]
        ],
        on="wikidata_id",
        how="left",
    )
    output["multi_context_type"] = output["context_count"].map(
        {2: "two_contexts", 3: "three_contexts", 4: "four_contexts"}
    )
    output["notes"] = "Context slices are non-exclusive; this table surfaces entities crossing reviewed contexts."
    return output.sort_values(["context_count", "wikipedia_sitelink_count"], ascending=[False, False])


def friction_counts(group: pd.DataFrame) -> list[dict]:
    rows = []
    denominator = group["wikidata_id"].nunique()
    if denominator == 0:
        return rows
    unique = group.drop_duplicates("wikidata_id")
    for metric, label, notes in FRICTION_METRICS:
        count = int(unique[metric].sum())
        rows.append(
            {
                "metric": metric,
                "metric_label": label,
                "entity_count": count,
                "denominator": denominator,
                "pct": percentage(count, denominator),
                "notes": notes,
            }
        )
    return rows


def group_friction_rows(base: pd.DataFrame, group_columns: list[str], aggregation_level: str) -> list[dict]:
    rows = []
    for keys, group in base.groupby(group_columns, dropna=False, sort=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        key_values = dict(zip(group_columns, keys))
        for metric_row in friction_counts(group):
            rows.append({**key_values, **metric_row, "aggregation_level": aggregation_level})
    return rows


def make_data_friction_by_context_gender_bucket(
    tables: dict[str, pd.DataFrame],
    entities: pd.DataFrame,
) -> pd.DataFrame:
    membership = membership_with_labels(tables["membership"])
    entity_columns = [
        "wikidata_id",
        "gender_category",
        *[metric for metric, _, _ in FRICTION_METRICS],
    ]
    base = membership[["wikidata_id", "slice_id", "context_label", "context_order"]].merge(
        entities[entity_columns],
        on="wikidata_id",
        how="left",
    )
    buckets = tables["occupation_entities"][
        ["wikidata_id", "granular_bucket", "bucket_family"]
    ].drop_duplicates()
    all_bucket_rows = base[["wikidata_id"]].drop_duplicates().assign(
        granular_bucket=ALL_BUCKETS,
        bucket_family="All occupation evidence",
    )
    buckets = pd.concat([buckets, all_bucket_rows], ignore_index=True).drop_duplicates()
    base = base.merge(buckets, on="wikidata_id", how="left")
    base["gender_category"] = base["gender_category"].fillna("unknown")

    rows = []
    context_columns = ["slice_id", "context_label", "context_order"]
    rows.extend(group_friction_rows(base.loc[base["granular_bucket"] == ALL_BUCKETS], context_columns, "context"))

    context_gender = base.loc[base["granular_bucket"] == ALL_BUCKETS].copy()
    rows.extend(
        group_friction_rows(
            context_gender,
            [*context_columns, "gender_category"],
            "context_gender",
        )
    )

    context_bucket = base.loc[base["granular_bucket"] != ALL_BUCKETS].copy()
    context_bucket["gender_category"] = ALL_GENDERS
    rows.extend(
        group_friction_rows(
            context_bucket,
            [*context_columns, "gender_category", "granular_bucket", "bucket_family"],
            "context_bucket",
        )
    )

    detailed_base = base.loc[base["granular_bucket"] != ALL_BUCKETS].copy()
    rows.extend(
        group_friction_rows(
            detailed_base,
            [*context_columns, "gender_category", "granular_bucket", "bucket_family"],
            "context_gender_bucket",
        )
    )

    output = pd.DataFrame(rows)
    output["granular_bucket"] = output["granular_bucket"].fillna(ALL_BUCKETS)
    output["bucket_family"] = output["bucket_family"].fillna("All occupation evidence")
    output["gender_category"] = output["gender_category"].fillna(ALL_GENDERS)
    return output.sort_values(
        ["context_order", "aggregation_level", "gender_category", "granular_bucket", "metric"]
    )


def entity_context_frame(tables: dict[str, pd.DataFrame], entities: pd.DataFrame) -> pd.DataFrame:
    membership = membership_with_labels(tables["membership"])
    context_lists = join_context_lists(membership)
    entity_context = entities.merge(context_lists, on="wikidata_id", how="left")
    entity_context["context_count"] = entity_context["context_count"].fillna(0).astype(int)
    entity_context["context_slice_ids"] = entity_context["context_slice_ids"].fillna("")
    entity_context["context_labels"] = entity_context["context_labels"].fillna("")
    return entity_context


def select_examples(
    frame: pd.DataFrame,
    mask,
    group_name: str,
    evidence_note: str,
    presenter_use: str,
    limit: int = 8,
) -> pd.DataFrame:
    selected = frame.loc[mask].copy()
    selected = selected.loc[~selected["name"].fillna("").str.match(r"^Q\d+$")]
    selected = selected.sort_values(
        ["context_count", "wikipedia_sitelink_count", "birth_year"],
        ascending=[False, False, True],
    ).head(limit)
    selected["example_group"] = group_name
    selected["evidence_note"] = evidence_note
    selected["presenter_use"] = presenter_use
    return selected


def make_example_entities(tables: dict[str, pd.DataFrame], entities: pd.DataFrame) -> pd.DataFrame:
    frame = entity_context_frame(tables, entities)
    membership = membership_with_labels(tables["membership"])

    british_place_only_ids = set(
        membership.loc[
            (membership["slice_id"] == "british")
            & (~membership["has_citizenship_evidence"].apply(bool_value))
            & (membership["has_place_context_evidence"].apply(bool_value)),
            "wikidata_id",
        ]
    )
    germany_place_only_ids = set(
        membership.loc[
            (membership["slice_id"] == "germany")
            & (~membership["has_citizenship_evidence"].apply(bool_value))
            & (membership["has_place_context_evidence"].apply(bool_value)),
            "wikidata_id",
        ]
    )
    china_citizenship_ids = set(
        membership.loc[
            (membership["slice_id"] == "china")
            & (membership["has_citizenship_evidence"].apply(bool_value)),
            "wikidata_id",
        ]
    )

    example_frames = [
        select_examples(
            frame,
            frame["context_count"].gt(1),
            "multi_context_high_visibility",
            "Appears in more than one reviewed context slice.",
            "Use to show that the ontology captures crossing affiliations, not just four isolated buckets.",
        ),
        select_examples(
            frame,
            frame["wikidata_id"].isin(british_place_only_ids),
            "british_place_only",
            "Recovered by British place context, not citizenship evidence.",
            "Use to explain why citizenship alone undercounts imperial or colonial context.",
        ),
        select_examples(
            frame,
            frame["wikidata_id"].isin(germany_place_only_ids),
            "germany_place_only",
            "Recovered by German place context, not citizenship evidence.",
            "Use to show that place context also matters beyond the British case.",
        ),
        select_examples(
            frame,
            frame["wikidata_id"].isin(china_citizenship_ids),
            "china_qing_citizenship_contrast",
            "Recovered through China/Qing citizenship evidence.",
            "Use as the contrast case where citizenship is comparatively orderly in current data.",
        ),
        select_examples(
            frame,
            frame["context_slice_ids"].str.contains("china", na=False)
            & frame["gender_category"].eq("female"),
            "women_in_china_qing_slice",
            "Female entities in the China/Qing slice.",
            "Use to open the gender-by-context discussion.",
        ),
        select_examples(
            frame,
            frame["missing_country_of_citizenship"] & frame["has_any_tracked_wikipedia_article"],
            "visible_but_missing_citizenship",
            "Has Wikipedia visibility but no country-of-citizenship value.",
            "Use to separate data visibility from clean structured citizenship.",
        ),
        select_examples(
            frame,
            frame["low_evidence_affiliation"] & frame["has_any_tracked_wikipedia_article"],
            "visible_but_low_evidence_affiliation",
            "Has Wikipedia visibility but only weak affiliation evidence.",
            "Use to show data friction as evidence, not just noise.",
        ),
    ]
    output = pd.concat(example_frames, ignore_index=True)
    columns = [
        "example_group",
        "wikidata_id",
        "name",
        "birth_year",
        "gender_category",
        "context_count",
        "context_labels",
        "wikipedia_sitelink_count",
        "best_candidate_affiliation",
        "top_candidate_affiliations",
        "occupation_labels",
        "evidence_note",
        "presenter_use",
    ]
    return output.loc[:, columns]


def metric_from_friction(friction: pd.DataFrame, scope_id: str, metric: str, column: str = "entity_count") -> object:
    match = friction.loc[(friction["scope_id"] == scope_id) & (friction["metric"] == metric)]
    if match.empty:
        return pd.NA
    return match[column].iloc[0]


def make_claim_candidates(
    tables: dict[str, pd.DataFrame],
    gender_context: pd.DataFrame,
    occupation_overrep: pd.DataFrame,
    decade_trends: pd.DataFrame,
    multi_context: pd.DataFrame,
) -> pd.DataFrame:
    context = tables["context"].set_index("slice_id")
    friction = tables["friction"]
    language = tables["language_by_slice"]

    missing_citizenship = metric_from_friction(friction, GLOBAL_SCOPE_ID, "missing_country_of_citizenship")
    missing_citizenship_pct = metric_from_friction(friction, GLOBAL_SCOPE_ID, "missing_country_of_citizenship", "pct")
    low_evidence = metric_from_friction(friction, GLOBAL_SCOPE_ID, "top_affiliation_single_field_or_less")
    low_evidence_pct = metric_from_friction(friction, GLOBAL_SCOPE_ID, "top_affiliation_single_field_or_less", "pct")

    female_rows = gender_context.loc[gender_context["gender_category"] == "female"].copy()
    female_high = female_rows.sort_values("slice_pct", ascending=False).iloc[0]
    female_low = female_rows.sort_values("slice_pct", ascending=True).iloc[0]
    female_global_pct = female_rows["global_pct"].dropna().iloc[0]

    top_occ = occupation_overrep.loc[
        (occupation_overrep["signal_class"].isin(["strong_overrepresented", "overrepresented"]))
        & (occupation_overrep["granular_bucket"].isin(CLAIM_OCCUPATION_BUCKETS))
    ].sort_values("index_vs_global", ascending=False).head(5)
    top_occ_text = "; ".join(
        f"{row.context_label}: {row.granular_bucket} ({row.index_vs_global:.2f}x)"
        for row in top_occ.itertuples(index=False)
    )

    language_top = language.sort_values("slice_to_global_representation_index", ascending=False).head(4)
    language_text = "; ".join(
        f"{row.review_group}: {row.language_edition} ({row.slice_to_global_representation_index:.2f}x)"
        for row in language_top.itertuples(index=False)
    )
    china_language = language.loc[language["slice_id"] == "china"].sort_values(
        "slice_to_global_representation_index",
        ascending=False,
    ).head(1)
    china_language_text = (
        f"China/Qing's highest tracked edition is {china_language.iloc[0].language_edition} "
        f"at {china_language.iloc[0].slice_to_global_representation_index:.2f}x global baseline"
        if not china_language.empty
        else "China/Qing has limited tracked language-edition coverage"
    )

    global_decades = decade_trends.loc[decade_trends["scope_id"] == GLOBAL_SCOPE_ID].sort_values("birth_decade")
    first_decade = global_decades.iloc[0]
    max_female_decade = global_decades.sort_values("female_pct", ascending=False).iloc[0]
    multi_count = len(multi_context)

    rows = [
        {
            "rank": 1,
            "claim_id": "evidence_constructs_context",
            "claim_title": "Historical context changes with the evidence field.",
            "claim_text": (
                f"Germany has {int(context.loc['germany', 'entities_with_place_context_only']):,} place-only entities, "
                f"France has {int(context.loc['france', 'entities_with_place_context_only']):,}, "
                f"British has {int(context.loc['british', 'entities_with_place_context_only']):,}, "
                f"while China/Qing has only {int(context.loc['china', 'entities_with_place_context_only']):,}."
            ),
            "evidence_table": "visual_matrix_evidence_construction.csv",
            "figure": "context_evidence_punchcard.svg",
            "signal_strength": "strong",
            "presenter_use": "Open with method: the same corpus produces different maps depending on evidence choice.",
            "caveat": "Context slices are non-exclusive and are constructed from reviewed crosswalk evidence.",
            "metadata_pull_needed": "no",
        },
        {
            "rank": 2,
            "claim_id": "citizenship_is_insufficient",
            "claim_title": "Country of citizenship cannot carry the argument by itself.",
            "claim_text": (
                f"{int(missing_citizenship):,} entities ({missing_citizenship_pct:.2f}%) lack citizenship; "
                f"{int(low_evidence):,} ({low_evidence_pct:.2f}%) have top affiliation support from one field or less."
            ),
            "evidence_table": "insight_data_friction_by_context_gender_bucket.csv",
            "figure": "data_friction_by_context.svg",
            "signal_strength": "strong",
            "presenter_use": "Frame data friction as part of the humanities method, not merely a cleanup defect.",
            "caveat": "Wikidata incompleteness varies by field, period, and language community.",
            "metadata_pull_needed": "no",
        },
        {
            "rank": 3,
            "claim_id": "gender_varies_by_context",
            "claim_title": "Gender representation is not flat across context slices.",
            "claim_text": (
                f"Female share is {female_high.slice_pct:.2f}% in {female_high.context_label}, "
                f"{female_low.slice_pct:.2f}% in {female_low.context_label}, "
                f"against {female_global_pct:.2f}% globally."
            ),
            "evidence_table": "insight_gender_context.csv",
            "figure": "gender_context_matrix.svg",
            "signal_strength": "strong",
            "presenter_use": "Move from identity construction into representation.",
            "caveat": "Gender categories are Wikidata-derived and should be interpreted cautiously.",
            "metadata_pull_needed": "no",
        },
        {
            "rank": 4,
            "claim_id": "occupation_profiles_differ",
            "claim_title": "Writerhood contains different intellectual profiles by context.",
            "claim_text": top_occ_text,
            "evidence_table": "insight_occupation_overrepresentation.csv",
            "figure": "occupation_overrepresentation_index.svg",
            "signal_strength": "strong",
            "presenter_use": "Show that occupation buckets turn a writer corpus into an intellectual network surface.",
            "caveat": "Occupation buckets are reviewable crosswalk categories, not natural kinds.",
            "metadata_pull_needed": "no",
        },
        {
            "rank": 5,
            "claim_id": "language_editions_shape_visibility",
            "claim_title": "Wikipedia language editions reshape visibility.",
            "claim_text": f"Top overrepresentation examples are {language_text}. {china_language_text}.",
            "evidence_table": "insight_gender_language_representation.csv",
            "figure": "language_representation_heatmap.svg",
            "signal_strength": "strong",
            "presenter_use": "Connect ontology mining to cross-language public representation.",
            "caveat": "Current evidence is article presence, not article length, framing, or pageviews.",
            "metadata_pull_needed": "maybe_later",
        },
        {
            "rank": 6,
            "claim_id": "birth_decades_add_motion",
            "claim_title": "Birth-decade trends add a time axis to the ontology.",
            "claim_text": (
                f"The corpus starts with {int(first_decade.entities):,} entities in the {int(first_decade.birth_decade)}s; "
                f"female share peaks at {max_female_decade.female_pct:.2f}% in the {int(max_female_decade.birth_decade)}s."
            ),
            "evidence_table": "insight_decade_trends.csv",
            "figure": "decade_trends.svg",
            "signal_strength": "strong",
            "presenter_use": "Use as the pacing shift from static matrices to historical movement.",
            "caveat": "Birth-decade rows reflect Wikidata cohort construction, not publication chronology.",
            "metadata_pull_needed": "no",
        },
        {
            "rank": 7,
            "claim_id": "multi_context_entities_expose_crossings",
            "claim_title": "Multi-context entities expose crossings between national and imperial frames.",
            "claim_text": f"{multi_count:,} entities appear in more than one reviewed context slice.",
            "evidence_table": "insight_multi_context_entities.csv",
            "figure": "multi_context_entities_matrix.svg",
            "signal_strength": "supporting",
            "presenter_use": "Use examples to humanize the matrices and show why non-exclusive slices matter.",
            "caveat": "Overlap means shared evidence context; it is not a claim of dual nationality.",
            "metadata_pull_needed": "no",
        },
        {
            "rank": 8,
            "claim_id": "metadata_gate",
            "claim_title": "The next data pull should be gated, not automatic.",
            "claim_text": "The current ontology yields at least six strong claims before article-depth metadata is needed.",
            "evidence_table": "insight_metadata_gap_assessment.csv",
            "figure": "",
            "signal_strength": "gate",
            "presenter_use": "Close with methodological discipline: pull more data only when a claim demands it.",
            "caveat": "Wikipedia article metadata may become useful if article presence is too coarse.",
            "metadata_pull_needed": "defer",
        },
    ]
    return pd.DataFrame(rows)


def make_metadata_gate(claims: pd.DataFrame) -> pd.DataFrame:
    strong_count = int((claims["signal_strength"] == "strong").sum())
    recommendation = "defer_wikipedia_metadata_pull" if strong_count >= 6 else "collect_wikipedia_metadata"
    return pd.DataFrame(
        [
            {
                "metric": "strong_claim_candidates",
                "value": strong_count,
                "threshold": 6,
                "recommendation": recommendation,
                "notes": "Collect metadata only if fewer than six strong claims exist from current data.",
            },
            {
                "metric": "article_presence_limitation",
                "value": "known",
                "threshold": "claim requires article depth",
                "recommendation": "collect_later_if_language_claim_needs_depth",
                "notes": "Article presence supports representation claims but not article framing, length, or attention.",
            },
            {
                "metric": "future_metadata_fields",
                "value": "page_id | article_length | lead_extract | categories | revision_count",
                "threshold": "existing sitelinks only",
                "recommendation": "scope_to_existing_wikipedia_sitelinks",
                "notes": "Do not broaden authority systems; enrich only pages already present in the sitelink matrix.",
            },
            {
                "metric": "phase_2_5_decision",
                "value": recommendation,
                "threshold": "Mine Then Gate",
                "recommendation": recommendation,
                "notes": "No new pull is part of this phase.",
            },
        ]
    )


def fmt_count(value) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{int(value):,}"


def fmt_pct(value) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{float(value):.2f}%"


def markdown_table(df: pd.DataFrame, columns: list[str], limit: int | None = None) -> str:
    display_df = df.loc[:, columns].head(limit).copy() if limit else df.loc[:, columns].copy()
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    rows = []
    for row in display_df.itertuples(index=False):
        values = []
        for value in row:
            if pd.isna(value):
                values.append("")
            elif isinstance(value, float):
                values.append(f"{value:.2f}".rstrip("0").rstrip("."))
            else:
                values.append(str(value).replace("|", r"\|"))
        rows.append("| " + " | ".join(values) + " |")
    return "\n".join([header, divider, *rows])


def build_markdown_report(outputs: dict[str, pd.DataFrame]) -> str:
    claims = outputs["claims"]
    gender = outputs["gender_context"]
    occupation = outputs["occupation_overrepresentation"]
    metadata = outputs["metadata_gate"]
    examples = outputs["examples"]

    female = gender.loc[gender["gender_category"] == "female"][
        ["context_label", "slice_entities", "slice_pct", "global_pct", "representation_index_vs_global"]
    ].copy()
    occupation_top = occupation.loc[
        (occupation["signal_class"].isin(["strong_overrepresented", "overrepresented"]))
        & (occupation["granular_bucket"].isin(CLAIM_OCCUPATION_BUCKETS))
    ].sort_values("index_vs_global", ascending=False)[
        ["context_label", "granular_bucket", "slice_entities", "slice_pct", "global_pct", "index_vs_global"]
    ].head(12)
    gate = metadata.loc[metadata["metric"] == "phase_2_5_decision", "recommendation"].iloc[0]

    return f"""# Insight Mining Packet

Generated from the current `global_writers` ontology. This packet does not
pull new data. It turns the existing Wikidata/Wikipedia evidence spine into a
conference-facing set of claim candidates for a 20-25 minute computational
humanities segment.

## TL;DR

- The current ontology yields six strong claim candidates before any new data
  pull is needed.
- The recommended metadata gate is `{gate}`.
- The strongest structure is: evidence construction, citizenship friction,
  gender representation, occupation/intellectual profile, language-edition
  visibility, decade trends, and multi-context examples.
- Context slices are non-exclusive. Overlap is a methodological feature, not a
  defect.

## Ranked Claim Candidates

{markdown_table(claims, ["rank", "claim_title", "signal_strength", "claim_text", "figure"])}

## Recommended 20-25 Minute Order

1. Start with the corpus and evidence construction matrix.
2. Use the citizenship/place punchcard to show why one field is not enough.
3. Move to the gender context matrix as the first representation result.
4. Show occupation overrepresentation to turn "writer" into intellectual
   profile.
5. Show language-edition representation as cross-language visibility.
6. Add birth-decade trends to give the ontology motion over time.
7. Use multi-context example entities to humanize the matrices.
8. Close with the metadata gate: pull more data only if article depth becomes
   necessary.

## Gender By Context

{markdown_table(female, list(female.columns))}

Suggested language: "Gender is not evenly distributed across the context
slices, and that difference is itself a computational finding. It asks us
whether the ontology, the source tradition, or the historical record is shaping
visibility."

## Occupation Overrepresentation

{markdown_table(occupation_top, list(occupation_top.columns))}

Suggested language: "The project does not just count writers. It decomposes the
writer cohort into intellectual labor: religion, education, print, politics,
law, science, philosophy, and the arts."

## Example Entities

{markdown_table(examples, ["example_group", "name", "birth_year", "gender_category", "context_labels", "wikipedia_sitelink_count"], limit=24)}

## Metadata Gate

{markdown_table(metadata, ["metric", "value", "threshold", "recommendation"])}

Current recommendation: do not pull Wikipedia article metadata yet. The next
pull should happen only if article presence is too coarse for the language
representation claim. If needed, the future pull should be limited to existing
sitelinks and should collect page ID, article length, lead extract, categories,
and revision count.

## Caveats

- Context slices are not mutually exclusive.
- Wikipedia article presence is visibility evidence, not article depth or
  cultural value.
- Gender, occupation, and affiliation categories are Wikidata-derived and
  should be described as structured-data evidence.
- Decade trends use birth year, not publication year or period of activity.
- Occupation buckets are reviewable crosswalk categories.
"""


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, DEFAULT_COHORT_ID)
    if paths.cohort_id != "global_writers":
        raise SystemExit("Insight mining expects global_writers to be the active analytical cohort.")

    tables = read_processed(paths)
    entities = clean_entities(tables)

    outputs = {
        "gender_context": make_gender_context(tables),
        "gender_language": make_gender_language_representation(tables, entities),
        "occupation_overrepresentation": make_occupation_overrepresentation(tables),
        "decade_trends": make_decade_trends(tables, entities),
        "multi_context": make_multi_context_entities(tables, entities),
        "data_friction": make_data_friction_by_context_gender_bucket(tables, entities),
        "examples": make_example_entities(tables, entities),
    }
    outputs["claims"] = make_claim_candidates(
        tables,
        outputs["gender_context"],
        outputs["occupation_overrepresentation"],
        outputs["decade_trends"],
        outputs["multi_context"],
    )
    outputs["metadata_gate"] = make_metadata_gate(outputs["claims"])

    for key, filename in OUTPUT_FILENAMES.items():
        outputs[key].to_csv(paths.processed_dir / filename, index=False)

    report_path = project_root / "docs" / "insight_mining_packet.md"
    report_path.write_text(build_markdown_report(outputs), encoding="utf-8")

    print("Insight mining packet complete.")
    print(f"Cohort: {paths.cohort_id}")
    print(f"Report: {report_path}")
    for filename in OUTPUT_FILENAMES.values():
        print(f"Output: {paths.processed_dir / filename}")


if __name__ == "__main__":
    main()
