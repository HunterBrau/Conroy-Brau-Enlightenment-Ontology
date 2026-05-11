"""
Build the first interpretation-ready analysis layer from the enriched cohort.

This script collapses the enriched Step 04 table to one row per Wikidata
entity, assigns provisional cultural-affiliation candidates from documented
Wikidata evidence fields, and creates language-edition representation matrices.

The affiliation mapping is intentionally conservative and auditable. It is not
a final nationality ontology; it is a first-pass scoring layer for comparing
representation patterns across Wikipedia language editions.
"""

from pathlib import Path
from argparse import ArgumentParser
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import DEFAULT_COHORT_ID, cohort_paths  # noqa: E402
from common import (  # noqa: E402
    EUROPEAN_LANGUAGE_CODES,
    WIKI_COLUMNS,
    bool_value,
    join_values,
    normalize_blank_strings,
    percentage,
    ratio,
    split_pipe_values,
)
from crosswalk import load_country_affiliation_mapping  # noqa: E402


PIPE_COLUMNS = [
    "birth_year",
    "birth_place",
    "occupation_raw",
    "citizenship_ids",
    "native_language_ids",
    "spoken_written_language_ids",
    "writing_language_ids",
    "gender_ids",
    "ethnic_group_ids",
    "occupation_ids",
    "occupation_labels",
    "writerly_occupation_ids",
    "nonwriter_occupation_ids",
    "entity_label_languages",
    "entity_description_languages",
]

NUMERIC_MAX_COLUMNS = [
    "duplicate_row_count",
    "wikipedia_sitelink_count",
    "european_label_count",
    "european_description_count",
]

BOOLEAN_ANY_COLUMNS = [
    "has_duplicate_wikidata_id",
    "name_is_qid",
    "birth_place_is_qid",
    "viaf_has_conflict",
]

SCORABLE_AFFILIATION_FAMILIES = [
    "citizenship",
    "native_language",
    "spoken_written_language",
    "writing_language",
]

COUNTRY_ID_TO_AFFILIATION = {
    "Q142": "French",
    "Q70972": "French",
    "Q71084": "French",
    "Q58296": "French",
    "Q207162": "French",
    "Q861551": "French",
    "Q39": "Swiss",
    "Q23366230": "Swiss",
    "Q3137802": "Swiss",
    "Q435583": "Swiss",
    "Q145": "Anglophone",
    "Q161885": "Anglophone",
    "Q174193": "Anglophone",
    "Q179876": "Anglophone",
    "Q215530": "Anglophone",
    "Q258532": "Anglophone",
    "Q179997": "Anglophone",
    "Q30": "Anglophone",
    "Q16": "Anglophone",
    "Q21": "Anglophone",
    "Q22": "Anglophone",
    "Q27": "Anglophone",
    "Q29": "Spanish",
    "Q80702": "Spanish",
    "Q298": "Spanish",
    "Q45670": "Portuguese",
    "Q55": "Dutch",
    "Q170072": "Dutch",
    "Q15864": "Dutch",
    "Q29999": "Dutch",
    "Q31": "Low Countries",
    "Q1031430": "Low Countries",
    "Q6581823": "Low Countries",
    "Q700283": "Low Countries",
    "Q158835": "Low Countries",
    "Q3456410": "Low Countries",
    "Q34": "Swedish",
    "Q756617": "Danish",
    "Q36": "Polish",
    "Q172107": "Polish",
    "Q221457": "Polish",
    "Q159": "Russian",
    "Q34266": "Russian",
    "Q186096": "Russian",
    "Q139319": "Russian",
    "Q2305208": "Russian",
    "Q15180": "Russian",
    "Q212": "Ukrainian",
    "Q148": "Chinese",
    "Q8733": "Chinese",
    "Q9903": "Chinese",
    "Q13426199": "Chinese",
    "Q696242": "Chinese",
    "Q704714": "Chinese",
    "Q814959": "Chinese",
    "Q183": "German",
    "Q38872": "German",
    "Q43287": "German",
    "Q41304": "German",
    "Q1206012": "German",
    "Q2415901": "German",
    "Q55300": "German",
    "Q7318": "German",
    "Q12548": "German",
    "Q27306": "German",
    "Q159631": "German",
    "Q2227570": "German",
    "Q164079": "German",
    "Q168651": "German",
    "Q186320": "German",
    "Q706691": "German",
    "Q830084": "German",
    "Q20135": "German",
    "Q22880": "German",
    "Q310650": "German",
    "Q315667": "German",
    "Q1055": "German",
    "Q40": "Austrian/Habsburg",
    "Q28513": "Austrian/Habsburg",
    "Q38": "Italian",
    "Q154849": "Italian",
    "Q165154": "Italian",
    "Q173065": "Italian",
    "Q4948": "Italian",
    "Q2577303": "Italian",
    "Q426025": "Italian",
    "Q170174": "Italian",
    "Q28": "Hungarian",
    "Q171150": "Hungarian",
    "Q213": "Czech",
    "Q42585": "Czech",
    "Q1998866": "Czech/Slovak",
    "Q33946": "Czech/Slovak",
    "Q853348": "Czech/Slovak",
    "Q85775800": "Czech",
    "Q131964": "Austrian/Habsburg",
    "Q153136": "Austrian/Habsburg",
    "Q10957559": "Romanian/Moldavian",
    "Q403": "Serbian",
    "Q214": "Slovak",
    "Q215": "Slovene",
    "Q41": "Greek",
    "Q45": "Portuguese",
    "Q35": "Danish",
    "Q191": "Estonian",
    "Q668": "South Asian",
    "Q79": "Egyptian",
    "Q8680": "British Empire/Colonial",
    "Q129286": "British Empire/Colonial",
    "Q2001966": "British Empire/Colonial",
    "Q208169": "Ragusan/Dalmatian",
    "Q37": "Lithuanian",
    "Q23498721": "Gold Coast/African",
    "Q790": "Haitian/Caribbean",
    "Q12560": "Ottoman",
    "Q491507": "Ottoman",
}

LANGUAGE_ID_TO_AFFILIATION = {
    "Q150": "French",
    "Q1860": "Anglophone",
    "Q652": "Italian",
    "Q188": "German",
    "Q1321": "Spanish",
    "Q7737": "Russian",
    "Q809": "Polish",
    "Q7411": "Dutch",
    "Q9027": "Swedish",
    "Q9035": "Danish",
    "Q9067": "Hungarian",
    "Q9129": "Greek",
    "Q36510": "Greek",
    "Q5146": "Portuguese",
    "Q9056": "Czech",
    "Q7913": "Romanian/Moldavian",
    "Q9299": "Serbian",
    "Q6654": "Croatian",
    "Q26245": "Ukrainian/Rusyn",
    "Q9058": "Slovak",
    "Q9063": "Slovene",
    "Q9072": "Estonian",
    "Q1412": "Finnish",
    "Q1450500": "Low Countries",
    "Q12107": "Breton",
    "Q14185": "Occitan",
    "Q2779185": "Occitan",
    "Q35735": "Occitan",
    "Q2736556": "Occitan",
    "Q427614": "Occitan",
    "Q942602": "Occitan",
    "Q510561": "French",
    "Q33302": "French",
    "Q7026": "Catalan",
    "Q8752": "Basque",
    "Q34219": "Walloon",
    "Q33111": "Corsican",
    "Q2479433": "Italian",
    "Q32724": "Italian",
    "Q36196": "Spanish",
    "Q8785": "Armenian",
    "Q9168": "Persian",
    "Q13955": "Arabic",
    "Q56426": "Arabic",
    "Q29919": "Arabic",
    "Q9288": "Hebrew",
    "Q8641": "Yiddish",
    "Q256": "Ottoman",
    "Q36730": "Ottoman",
    "Q727694": "Chinese",
}

TOKEN_LABEL_OVERRIDES = {
    "Q11059": "Sanskrit",
    "Q1248221": "Neo-Latin",
    "Q28602": "Aramaic",
    "Q33538": "Syriac",
    "Q33578": "Igbo",
    "Q34057": "Tagalog",
    "Q35497": "Ancient Greek",
    "Q397": "Latin",
    "Q56612": "Samaritan Aramaic",
    "Q589662": "Royal Prussia",
}


def split_occupation_tokens(value) -> list[str]:
    tokens = []
    for pipe_value in split_pipe_values(value):
        tokens.extend(token.strip() for token in pipe_value.split(",") if token.strip())
    return sorted(set(tokens))


def bool_from_cell(value) -> bool:
    return bool_value(value)


def join_unique_values(values) -> object:
    return join_values(values, sort=True)


def join_pipe_values(values) -> object:
    cleaned = sorted({token for value in values for token in split_pipe_values(value)})
    if not cleaned:
        return pd.NA
    return " | ".join(cleaned)


def load_name_corrections(project_root: Path) -> dict[str, str]:
    corrections_path = project_root / "data" / "processed" / "person_name_label_corrections.csv"
    if not corrections_path.exists():
        return {}

    corrections_df = pd.read_csv(corrections_path)
    return dict(
        zip(
            corrections_df["wikidata_id"].astype(str),
            corrections_df["resolved_name"].astype(str),
            strict=False,
        )
    )


def choose_display_name(group: pd.DataFrame, corrections: dict[str, str]) -> str:
    wikidata_id = str(group["wikidata_id"].iloc[0])
    if wikidata_id in corrections:
        return corrections[wikidata_id]

    names = [
        str(name).strip()
        for name in group["name"].dropna()
        if str(name).strip() and not str(name).strip().startswith("Q")
    ]
    if names:
        return sorted(set(names))[0]

    if "wikidata_enrichment_label" in group.columns:
        enrichment_names = [
            str(name).strip()
            for name in group["wikidata_enrichment_label"].dropna()
            if str(name).strip() and not str(name).strip().startswith("Q")
        ]
        if enrichment_names:
            return sorted(set(enrichment_names))[0]

    fallback = group["name"].dropna().astype(str)
    if not fallback.empty:
        return fallback.iloc[0]
    return wikidata_id.rsplit("/", 1)[-1]


def build_entity_table(df: pd.DataFrame, corrections: dict[str, str]) -> pd.DataFrame:
    rows = []

    for wikidata_id, group in df.groupby("wikidata_id", dropna=False):
        row = {
            "wikidata_id": wikidata_id,
            "name": choose_display_name(group, corrections),
            "source_row_count": len(group),
        }

        for column in PIPE_COLUMNS:
            if column in group.columns:
                if column in {"birth_year", "birth_place"}:
                    row[f"{column}_values"] = join_unique_values(group[column])
                else:
                    row[column] = join_pipe_values(group[column])

        for column in NUMERIC_MAX_COLUMNS:
            if column in group.columns:
                numeric = pd.to_numeric(group[column], errors="coerce").dropna()
                row[column] = int(numeric.max()) if not numeric.empty else pd.NA

        for column in BOOLEAN_ANY_COLUMNS:
            if column in group.columns:
                row[column] = any(bool_from_cell(value) for value in group[column])

        for wiki_code, column in WIKI_COLUMNS.items():
            row[column] = any(bool_from_cell(value) for value in group[column])

        rows.append(row)

    entity_df = pd.DataFrame(rows).sort_values(["name", "wikidata_id"])
    entity_df["gender_category"] = entity_df["gender_ids"].apply(classify_gender)
    if "occupation_raw" not in entity_df.columns:
        entity_df["occupation_raw"] = pd.NA
    if "occupation_labels" not in entity_df.columns:
        entity_df["occupation_labels"] = pd.NA
    entity_df["occupation_raw_tokens"] = entity_df.apply(
        lambda row: " | ".join(
            split_occupation_tokens(row["occupation_raw"])
            or split_pipe_values(row["occupation_labels"])
        ) or pd.NA,
        axis=1,
    )
    return entity_df


def load_place_affiliation_context(processed_dir: Path) -> pd.DataFrame:
    place_path = processed_dir / "place_affiliation_best_candidates.csv"
    if not place_path.exists():
        return pd.DataFrame()

    place_df = pd.read_csv(place_path)
    keep_columns = [
        "wikidata_id",
        "place_evidence_role_count",
        "mapped_place_role_count",
        "place_candidate_affiliation_count",
        "best_place_candidate_affiliation",
        "top_place_candidate_affiliations",
        "top_place_checked_role_count",
        "second_place_checked_role_count",
        "top_place_score_share",
        "top_place_candidate_tie_count",
        "place_affiliation_review_status",
    ]
    return place_df[[column for column in keep_columns if column in place_df.columns]].drop_duplicates("wikidata_id")


def classify_place_affiliation_category(row) -> str:
    review_status = row.get("place_affiliation_review_status")
    best_affiliation = row.get("best_place_candidate_affiliation")
    evidence_count = row.get("place_evidence_role_count")

    if pd.isna(evidence_count):
        return "no_place_context"
    if review_status == "mixed_place_top_score":
        return "mixed_or_ambiguous"
    if pd.notna(best_affiliation):
        return best_affiliation
    return "no_mapped_place_affiliation"


def append_place_affiliation_context(entity_df: pd.DataFrame, processed_dir: Path) -> pd.DataFrame:
    place_df = load_place_affiliation_context(processed_dir)
    if place_df.empty:
        entity_df["place_affiliation_category"] = "no_place_context"
        return entity_df

    merged = entity_df.merge(place_df, on="wikidata_id", how="left")
    merged["place_affiliation_category"] = merged.apply(classify_place_affiliation_category, axis=1)
    return merged


def classify_gender(gender_ids) -> str:
    ids = set(split_pipe_values(gender_ids))
    if ids == {"Q6581097"}:
        return "male"
    if ids == {"Q6581072"}:
        return "female"
    if not ids:
        return "unknown"
    if len(ids) > 1:
        return "multiple_or_ambiguous"
    return "other_or_unmapped"


def collect_affiliations(tokens: list[str], mapping: dict[str, str]) -> tuple[set[str], list[str]]:
    affiliations = set()
    unmapped = []
    for token in tokens:
        affiliation = mapping.get(token)
        if affiliation:
            affiliations.add(affiliation)
        else:
            unmapped.append(token)
    return affiliations, unmapped


def classify_affiliation_review_status(top_score: int, second_score: int, tie_count: int) -> str:
    if top_score == 0:
        return "no_mapped_affiliation"
    if tie_count > 1:
        return "mixed_top_score"
    if top_score >= 3 and (top_score - second_score) >= 2:
        return "strong"
    if top_score >= 3 and (top_score - second_score) >= 1:
        return "moderate"
    if top_score >= 2:
        return "tentative"
    return "single_signal"


def build_affiliation_tables(
    entity_df: pd.DataFrame,
    country_mapping: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    candidate_rows = []
    best_rows = []
    unmapped_rows = []

    family_specs = {
        "citizenship": ("citizenship_ids", country_mapping),
        "native_language": ("native_language_ids", LANGUAGE_ID_TO_AFFILIATION),
        "spoken_written_language": ("spoken_written_language_ids", LANGUAGE_ID_TO_AFFILIATION),
        "writing_language": ("writing_language_ids", LANGUAGE_ID_TO_AFFILIATION),
    }

    for row in entity_df.itertuples(index=False):
        row_dict = row._asdict()
        family_affiliations = {}

        for family, (column, mapping) in family_specs.items():
            affiliations, unmapped = collect_affiliations(
                split_pipe_values(row_dict.get(column)),
                mapping,
            )
            family_affiliations[family] = affiliations
            for token in unmapped:
                unmapped_rows.append(
                    {
                        "wikidata_id": row.wikidata_id,
                        "name": row.name,
                        "token_family": family,
                        "token_id": token,
                        "token_label": TOKEN_LABEL_OVERRIDES.get(token, pd.NA),
                        "token_wikidata_url": f"https://www.wikidata.org/wiki/{token}",
                    }
                )

        candidates = sorted(
            {
                affiliation
                for affiliations in family_affiliations.values()
                for affiliation in affiliations
            }
        )
        possible_boxes = sum(bool(family_affiliations[family]) for family in SCORABLE_AFFILIATION_FAMILIES)

        person_candidate_rows = []
        for candidate in candidates:
            candidate_row = {
                "wikidata_id": row.wikidata_id,
                "name": row.name,
                "candidate_affiliation": candidate,
                "possible_box_count": possible_boxes,
            }
            checked_boxes = 0
            for family in SCORABLE_AFFILIATION_FAMILIES:
                support = candidate in family_affiliations[family]
                candidate_row[f"{family}_support"] = support
                checked_boxes += int(support)
            candidate_row["checked_box_count"] = checked_boxes
            candidate_row["score_share"] = ratio(checked_boxes, possible_boxes)
            person_candidate_rows.append(candidate_row)

        candidate_rows.extend(person_candidate_rows)
        if person_candidate_rows:
            sorted_candidates = sorted(
                person_candidate_rows,
                key=lambda item: (
                    -item["checked_box_count"],
                    -(item["score_share"] if pd.notna(item["score_share"]) else -1),
                    item["candidate_affiliation"],
                ),
            )
            top_score = int(sorted_candidates[0]["checked_box_count"])
            second_score = int(sorted_candidates[1]["checked_box_count"]) if len(sorted_candidates) > 1 else 0
            top_candidates = [
                item["candidate_affiliation"]
                for item in sorted_candidates
                if item["checked_box_count"] == top_score
            ]
            best_candidate = top_candidates[0] if len(top_candidates) == 1 else pd.NA
            review_status = classify_affiliation_review_status(top_score, second_score, len(top_candidates))
            top_score_share = ratio(top_score, possible_boxes)
        else:
            top_score = 0
            second_score = 0
            top_candidates = []
            best_candidate = pd.NA
            review_status = "no_mapped_affiliation"
            top_score_share = pd.NA

        best_rows.append(
            {
                "wikidata_id": row.wikidata_id,
                "name": row.name,
                "candidate_affiliation_count": len(person_candidate_rows),
                "best_candidate_affiliation": best_candidate,
                "top_candidate_affiliations": " | ".join(top_candidates) if top_candidates else pd.NA,
                "top_checked_box_count": top_score,
                "second_checked_box_count": second_score,
                "top_score_share": top_score_share,
                "top_candidate_tie_count": len(top_candidates),
                "affiliation_review_status": review_status,
            }
        )

    candidate_df = pd.DataFrame(candidate_rows)
    best_df = pd.DataFrame(best_rows)
    unmapped_df = pd.DataFrame(unmapped_rows)
    if not unmapped_df.empty:
        unmapped_df = (
            unmapped_df.groupby(["token_family", "token_id"], dropna=False)
            .agg(
                token_label=("token_label", "first"),
                token_wikidata_url=("token_wikidata_url", "first"),
                occurrence_count=("token_id", "size"),
                entity_count=("wikidata_id", "nunique"),
                example_names=("name", lambda values: " | ".join(sorted(set(values))[:8])),
            )
            .reset_index()
            .sort_values(["occurrence_count", "token_family", "token_id"], ascending=[False, True, True])
        )

    return candidate_df, best_df, unmapped_df


def build_wikipedia_long(entity_df: pd.DataFrame, best_affiliation_df: pd.DataFrame) -> pd.DataFrame:
    base = entity_df.merge(best_affiliation_df, on=["wikidata_id", "name"], how="left")
    rows = []

    for row in base.itertuples(index=False):
        row_dict = row._asdict()
        if row.affiliation_review_status == "mixed_top_score":
            affiliation_category = "mixed_or_ambiguous"
        elif pd.isna(row.best_candidate_affiliation):
            affiliation_category = "no_mapped_affiliation"
        else:
            affiliation_category = row.best_candidate_affiliation

        for wiki_code, column in WIKI_COLUMNS.items():
            rows.append(
                {
                    "wikidata_id": row.wikidata_id,
                    "name": row.name,
                    "language_edition": wiki_code,
                    "has_article": bool(row_dict[column]),
                    "wikipedia_sitelink_count": row.wikipedia_sitelink_count,
                    "gender_category": row.gender_category,
                    "affiliation_category": affiliation_category,
                    "place_affiliation_category": row_dict.get("place_affiliation_category", pd.NA),
                    "best_place_candidate_affiliation": row_dict.get("best_place_candidate_affiliation", pd.NA),
                    "top_place_candidate_affiliations": row_dict.get("top_place_candidate_affiliations", pd.NA),
                    "place_affiliation_review_status": row_dict.get("place_affiliation_review_status", pd.NA),
                    "top_candidate_affiliations": row.top_candidate_affiliations,
                    "affiliation_review_status": row.affiliation_review_status,
                    "has_duplicate_wikidata_id": row.has_duplicate_wikidata_id,
                    "viaf_has_conflict": row.viaf_has_conflict,
                }
            )

    return pd.DataFrame(rows)


def build_language_summary(wikipedia_long_df: pd.DataFrame) -> pd.DataFrame:
    total_entities = wikipedia_long_df["wikidata_id"].nunique()
    rows = []
    for language_edition, group in wikipedia_long_df.groupby("language_edition"):
        represented = int(group["has_article"].sum())
        rows.append(
            {
                "language_edition": language_edition,
                "total_entities": total_entities,
                "represented_entities": represented,
                "representation_pct": percentage(represented, total_entities),
            }
        )
    return pd.DataFrame(rows).sort_values("represented_entities", ascending=False)


def build_group_summary(
    wikipedia_long_df: pd.DataFrame,
    assignments_df: pd.DataFrame,
    group_field: str,
) -> pd.DataFrame:
    total_entities = wikipedia_long_df["wikidata_id"].nunique()
    language_totals = (
        wikipedia_long_df.loc[wikipedia_long_df["has_article"]]
        .groupby("language_edition")["wikidata_id"]
        .nunique()
        .rename("language_represented_entities")
        .reset_index()
    )
    group_totals = (
        assignments_df.groupby(group_field)["wikidata_id"]
        .nunique()
        .rename("cohort_group_entities")
        .reset_index()
    )
    merged = wikipedia_long_df.drop(columns=[group_field], errors="ignore").merge(
        assignments_df,
        on="wikidata_id",
        how="inner",
    )
    represented = (
        merged.loc[merged["has_article"]]
        .groupby(["language_edition", group_field])["wikidata_id"]
        .nunique()
        .rename("represented_entities")
        .reset_index()
    )

    grid = (
        pd.MultiIndex.from_product(
            [
                sorted(wikipedia_long_df["language_edition"].unique()),
                sorted(assignments_df[group_field].dropna().unique()),
            ],
            names=["language_edition", group_field],
        )
        .to_frame(index=False)
        .merge(represented, on=["language_edition", group_field], how="left")
        .merge(group_totals, on=group_field, how="left")
        .merge(language_totals, on="language_edition", how="left")
    )
    grid["represented_entities"] = grid["represented_entities"].fillna(0).astype(int)
    grid["language_represented_entities"] = grid["language_represented_entities"].fillna(0).astype(int)
    grid["cohort_total_entities"] = total_entities
    grid["cohort_group_share_pct"] = grid["cohort_group_entities"].apply(
        lambda count: percentage(int(count), total_entities)
    )
    grid["language_group_share_pct"] = grid.apply(
        lambda row: percentage(int(row["represented_entities"]), int(row["language_represented_entities"])),
        axis=1,
    )
    grid["representation_rate_pct"] = grid.apply(
        lambda row: percentage(int(row["represented_entities"]), int(row["cohort_group_entities"])),
        axis=1,
    )
    grid["representation_index"] = grid.apply(
        lambda row: (
            round(row["language_group_share_pct"] / row["cohort_group_share_pct"], 4)
            if row["cohort_group_share_pct"] > 0
            else pd.NA
        ),
        axis=1,
    )
    return grid.sort_values(["language_edition", "represented_entities"], ascending=[True, False])


def build_gender_assignments(entity_df: pd.DataFrame) -> pd.DataFrame:
    return entity_df[["wikidata_id", "gender_category"]].drop_duplicates()


def build_affiliation_assignments(best_affiliation_df: pd.DataFrame) -> pd.DataFrame:
    assignments = best_affiliation_df[["wikidata_id", "best_candidate_affiliation", "affiliation_review_status"]].copy()
    assignments["affiliation_category"] = assignments.apply(
        lambda row: (
            "mixed_or_ambiguous"
            if row["affiliation_review_status"] == "mixed_top_score"
            else (
                row["best_candidate_affiliation"]
                if pd.notna(row["best_candidate_affiliation"])
                else "no_mapped_affiliation"
            )
        ),
        axis=1,
    )
    return assignments[["wikidata_id", "affiliation_category"]].drop_duplicates()


def build_place_affiliation_assignments(entity_df: pd.DataFrame) -> pd.DataFrame:
    if "place_affiliation_category" not in entity_df.columns:
        return pd.DataFrame(
            {
                "wikidata_id": entity_df["wikidata_id"],
                "place_affiliation_category": "no_place_context",
            }
        ).drop_duplicates()
    return entity_df[["wikidata_id", "place_affiliation_category"]].drop_duplicates()


def build_occupation_assignments(entity_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in entity_df.itertuples(index=False):
        occupations = split_occupation_tokens(row.occupation_raw)
        if not occupations and hasattr(row, "occupation_labels"):
            occupations = split_pipe_values(row.occupation_labels)
        for occupation in occupations:
            rows.append({"wikidata_id": row.wikidata_id, "occupation": occupation})
    if not rows:
        return pd.DataFrame(columns=["wikidata_id", "occupation"])
    return pd.DataFrame(rows).drop_duplicates()


def build_label_coverage_summary(entity_df: pd.DataFrame, language_summary_df: pd.DataFrame) -> pd.DataFrame:
    total_entities = entity_df["wikidata_id"].nunique()
    wiki_lookup = language_summary_df.set_index("language_edition")["represented_entities"].to_dict()
    rows = []
    for language_code in EUROPEAN_LANGUAGE_CODES:
        label_count = int(entity_df["entity_label_languages"].apply(lambda value: language_code in split_pipe_values(value)).sum())
        description_count = int(
            entity_df["entity_description_languages"].apply(lambda value: language_code in split_pipe_values(value)).sum()
        )
        wiki_code = f"{language_code}wiki"
        rows.append(
            {
                "language_code": language_code,
                "wiki_edition": wiki_code,
                "wikidata_label_entities": label_count,
                "wikidata_label_pct": percentage(label_count, total_entities),
                "wikidata_description_entities": description_count,
                "wikidata_description_pct": percentage(description_count, total_entities),
                "wikipedia_article_entities": int(wiki_lookup.get(wiki_code, 0)),
                "wikipedia_article_pct": percentage(int(wiki_lookup.get(wiki_code, 0)), total_entities),
            }
        )
    return pd.DataFrame(rows).sort_values("wikidata_label_entities", ascending=False)


def build_manifest(project_root: Path, outputs: list[tuple[Path, str]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "path": path.relative_to(project_root).as_posix(),
                "description": description,
                "format": path.suffix.lstrip("."),
            }
            for path, description in outputs
        ]
    )


def parse_args() -> object:
    parser = ArgumentParser(description="Build representation matrices for a cohort.")
    parser.add_argument(
        "--cohort-id",
        default=DEFAULT_COHORT_ID,
        choices=["french_seed", "global_writers"],
        help=f"Cohort to analyze. Default: {DEFAULT_COHORT_ID}.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, args.cohort_id)
    input_path = paths.enriched_path
    output_dir = paths.processed_dir

    if not input_path.exists():
        raise SystemExit(
            "Missing enriched cohort. Run Step 04 first:\n"
            f"python scripts/pipeline/04_merge_wikidata_enrichment.py --cohort-id {paths.cohort_id}"
        )

    df = normalize_blank_strings(pd.read_csv(input_path))
    corrections = load_name_corrections(project_root)
    country_mapping = load_country_affiliation_mapping(project_root, COUNTRY_ID_TO_AFFILIATION)
    entity_df = build_entity_table(df, corrections)
    candidate_df, best_affiliation_df, unmapped_df = build_affiliation_tables(entity_df, country_mapping)
    entity_df = append_place_affiliation_context(entity_df, output_dir)
    wikipedia_long_df = build_wikipedia_long(entity_df, best_affiliation_df)
    language_summary_df = build_language_summary(wikipedia_long_df)

    gender_summary_df = build_group_summary(
        wikipedia_long_df=wikipedia_long_df,
        assignments_df=build_gender_assignments(entity_df),
        group_field="gender_category",
    )
    affiliation_summary_df = build_group_summary(
        wikipedia_long_df=wikipedia_long_df,
        assignments_df=build_affiliation_assignments(best_affiliation_df),
        group_field="affiliation_category",
    )
    place_affiliation_summary_df = build_group_summary(
        wikipedia_long_df=wikipedia_long_df,
        assignments_df=build_place_affiliation_assignments(entity_df),
        group_field="place_affiliation_category",
    )
    occupation_summary_df = build_group_summary(
        wikipedia_long_df=wikipedia_long_df,
        assignments_df=build_occupation_assignments(entity_df),
        group_field="occupation",
    )
    label_coverage_df = build_label_coverage_summary(entity_df, language_summary_df)

    output_paths = [
        (output_dir / "representation_entities.csv", "One row per distinct Wikidata entity with analysis categories."),
        (output_dir / "cultural_affiliation_candidates_long.csv", "Candidate affiliation support rows."),
        (output_dir / "cultural_affiliation_best_candidates.csv", "Best affiliation summary per entity."),
        (output_dir / "cultural_affiliation_unmapped_tokens.csv", "Unmapped country/language tokens for affiliation scoring."),
        (output_dir / "wikipedia_representation_long.csv", "One row per entity and Wikipedia language edition."),
        (output_dir / "representation_language_summary.csv", "Overall representation by Wikipedia language edition."),
        (output_dir / "representation_by_gender.csv", "Wikipedia representation by gender category."),
        (output_dir / "representation_by_affiliation.csv", "Wikipedia representation by inferred affiliation category."),
        (output_dir / "representation_by_place_affiliation.csv", "Wikipedia representation by place-derived affiliation category."),
        (output_dir / "representation_by_occupation.csv", "Wikipedia representation by raw occupation token."),
        (output_dir / "wikidata_label_coverage_by_language.csv", "Wikidata label/description coverage by language."),
        (output_dir / "representation_analysis_manifest.csv", "Inventory of analysis outputs."),
    ]
    manifest_df = build_manifest(project_root, output_paths)

    output_dir.mkdir(parents=True, exist_ok=True)
    entity_df.to_csv(output_paths[0][0], index=False)
    candidate_df.to_csv(output_paths[1][0], index=False)
    best_affiliation_df.to_csv(output_paths[2][0], index=False)
    unmapped_df.to_csv(output_paths[3][0], index=False)
    wikipedia_long_df.to_csv(output_paths[4][0], index=False)
    language_summary_df.to_csv(output_paths[5][0], index=False)
    gender_summary_df.to_csv(output_paths[6][0], index=False)
    affiliation_summary_df.to_csv(output_paths[7][0], index=False)
    place_affiliation_summary_df.to_csv(output_paths[8][0], index=False)
    occupation_summary_df.to_csv(output_paths[9][0], index=False)
    label_coverage_df.to_csv(output_paths[10][0], index=False)
    manifest_df.to_csv(output_paths[11][0], index=False)

    print("Representation matrices complete.")
    print(f"Cohort: {paths.cohort_id}")
    print(f"Input dataset: {input_path}")
    print(f"Entity rows: {len(entity_df)}")
    print(f"Wikipedia long rows: {len(wikipedia_long_df)}")
    print(f"Affiliation candidate rows: {len(candidate_df)}")
    print(f"Outputs written: {len(output_paths)}")
    print()
    print("Top language editions by represented entities:")
    print(language_summary_df.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
