"""
Build geographic-scope diagnostics for the 1675-1775 writer cohort.

This script answers three closely related questions:

- how many current cohort entities lack country-of-citizenship evidence;
- whether China and the British Empire appear through citizenship or through
  contextual place evidence instead;
- how far the current cohort can be split into European, non-European, colonial,
  mixed, or unresolved birth-place contexts.

The scope classification is deliberately conservative. It uses birth-place
context as the primary "from" signal and keeps transcontinental or imperial
contexts separate instead of forcing them into a binary Europe/non-Europe split.
"""

from pathlib import Path
from argparse import ArgumentParser
import sys
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import DEFAULT_COHORT_ID, cohort_paths  # noqa: E402
from common import (  # noqa: E402
    join_values,
    normalize_blank_strings,
    ordered_unique,
    percentage,
    qid_from_uri,
    split_pipe_values,
)
from crosswalk import build_reviewed_scope_sets, load_token_label_overrides  # noqa: E402


TOKEN_COLUMNS = [
    ("direct_country_ids", "direct_country_labels"),
    ("admin_country_ids", "admin_country_labels"),
    ("context_country_ids", "context_country_labels"),
    ("admin_entity_ids", "admin_entity_labels"),
]

CHINA_CONTEXT_IDS = {
    "Q148",  # People's Republic of China
    "Q8733",  # Qing dynasty
    "Q9903",  # Ming dynasty
    "Q13426199",  # Republic of China
    "Q696242",  # Beiyang government
    "Q704714",  # Provisional Government of the Republic of China
    "Q814959",  # Reorganized National Government of the Republic of China
}

BRITISH_EMPIRE_CONTEXT_IDS = {
    "Q8680",  # British Empire
    "Q129286",  # British Raj
    "Q2001966",  # Company rule in India
}

NON_EUROPE_GEOGRAPHIC_IDS = {
    "Q16",  # Canada
    "Q30",  # United States
    "Q79",  # Egypt
    "Q96",  # Mexico
    "Q148",  # China
    "Q155",  # Brazil
    "Q258",  # South Africa
    "Q298",  # Chile
    "Q414",  # Argentina
    "Q419",  # Peru
    "Q668",  # India
    "Q730",  # Suriname
    "Q736",  # Ecuador
    "Q739",  # Colombia
    "Q790",  # Haiti
    "Q1028",  # Morocco
    "Q1027",  # Mauritius
    "Q117",  # Ghana
    "Q962",  # Benin
    "Q23498721",  # Gold Coast
    "Q3916279",  # Macau
    "Q956",  # Beijing
    *CHINA_CONTEXT_IDS,
}

COLONIAL_CONTEXT_IDS = {
    "Q8680",  # British Empire
    "Q129286",  # British Raj
    "Q2001966",  # Company rule in India
    "Q258532",  # British America
    "Q179997",  # Thirteen Colonies
}

TRANSCONTINENTAL_OR_IMPERIAL_IDS = {
    "Q159",  # Russia
    "Q34266",  # Russian Empire
    "Q186096",  # Tsardom of Russia
    "Q139319",  # Soviet Union
    "Q2305208",  # Russian Republic
    "Q15180",  # Grand Duchy of Moscow
    "Q12560",  # Ottoman Empire
    "Q1408764",  # Circassia
    "Q491507",  # Ottoman Algeria
}

EUROPE_GEOGRAPHIC_IDS = {
    "Q21",  # England
    "Q22",  # Scotland
    "Q27",  # Ireland
    "Q28",  # Hungary
    "Q29",  # Spain
    "Q31",  # Belgium
    "Q34",  # Sweden
    "Q35",  # Denmark
    "Q36",  # Poland
    "Q37",  # Lithuania
    "Q38",  # Italy
    "Q39",  # Switzerland
    "Q40",  # Austria
    "Q41",  # Greece
    "Q45",  # Portugal
    "Q55",  # Netherlands
    "Q211",  # Latvia
    "Q235",  # Monaco
    "Q236",  # Montenegro
    "Q142",  # France
    "Q145",  # United Kingdom
    "Q183",  # Germany
    "Q191",  # Estonia
    "Q212",  # Ukraine
    "Q213",  # Czech Republic
    "Q214",  # Slovakia
    "Q215",  # Slovenia
    "Q403",  # Serbia
    "Q45670",  # Kingdom of Portugal
    "Q58296",  # First French Empire
    "Q70972",  # Kingdom of France
    "Q71084",  # French First Republic
    "Q756617",  # Denmark-Norway
    "Q80702",  # Spanish Empire, kept visible as an imperial European state
    "Q1031430",  # Austrian Netherlands
    "Q10957559",  # Moldavia
    "Q1206012",  # Kingdom of Saxony
    "Q131964",  # Principality of Transylvania
    "Q153136",  # Habsburg monarchy
    "Q154849",  # Republic of Venice
    "Q15864",  # Dutch Republic
    "Q158835",  # Spanish Netherlands
    "Q159631",  # Duchy of Württemberg
    "Q161885",  # Kingdom of Great Britain
    "Q164079",  # Kingdom of Bavaria
    "Q165154",  # Kingdom of Sardinia
    "Q168651",  # Duchy of Brunswick-Lüneburg
    "Q170072",  # Kingdom of the Netherlands
    "Q170174",  # Papal States
    "Q171150",  # Kingdom of Hungary
    "Q172107",  # Polish-Lithuanian Commonwealth
    "Q173065",  # Kingdom of Naples
    "Q174193",  # United Kingdom of Great Britain and Ireland
    "Q179876",  # Kingdom of England
    "Q1998866",  # Czechoslovakia / Czech-Slovak context
    "Q20135",  # Duchy of Courland and Semigallia
    "Q207162",  # Bourbon Restoration
    "Q208169",  # Republic of Ragusa
    "Q215530",  # Kingdom of Ireland
    "Q221457",  # Congress Poland
    "Q2227570",  # Electorate of Mainz
    "Q23366230",  # Old Swiss Confederacy
    "Q2415901",  # Electorate of Hanover
    "Q2577303",  # Grand Duchy of Tuscany
    "Q27306",  # Kingdom of Prussia
    "Q28513",  # Austrian Empire
    "Q3137802",  # Helvetic Republic
    "Q315667",  # Duchy of Parma
    "Q33946",  # Bohemia
    "Q3456410",  # United Belgian States
    "Q38872",  # Holy Roman Empire
    "Q41304",  # Kingdom of Saxony
    "Q42585",  # Kingdom of Bohemia
    "Q426025",  # Duchy of Milan
    "Q43287",  # Prussia
    "Q435583",  # Republic and Canton of Geneva
    "Q4948",  # Kingdom of Sicily
    "Q55300",  # Electorate of Bavaria
    "Q6581823",  # Southern Netherlands
    "Q700283",  # Austrian Netherlands
    "Q706691",  # Mecklenburg-Schwerin
    "Q7318",  # Electorate of Saxony
    "Q830084",  # Saxe-Weimar
    "Q853348",  # Moravia
    "Q85775800",  # Czech context
    "Q861551",  # July Monarchy
    "Q1055",  # Hamburg
}

SPECIAL_CONTEXTS = {
    "China/Qing context": CHINA_CONTEXT_IDS,
    "British Empire/colonial context": BRITISH_EMPIRE_CONTEXT_IDS,
}

TOKEN_LABEL_OVERRIDES = {
    "Q16": "Canada",
    "Q30": "United States",
    "Q79": "Egypt",
    "Q96": "Mexico",
    "Q117": "Ghana",
    "Q148": "China",
    "Q155": "Brazil",
    "Q258": "South Africa",
    "Q298": "Chile",
    "Q414": "Argentina",
    "Q419": "Peru",
    "Q668": "India",
    "Q730": "Suriname",
    "Q736": "Ecuador",
    "Q739": "Colombia",
    "Q790": "Haiti",
    "Q962": "Benin",
    "Q1027": "Mauritius",
    "Q956": "Beijing",
    "Q1028": "Morocco",
    "Q1408764": "Circassia",
    "Q8733": "Qing dynasty",
    "Q9903": "Ming dynasty",
    "Q8680": "British Empire",
    "Q129286": "British Raj",
    "Q179997": "Thirteen Colonies",
    "Q2001966": "Company rule in India",
    "Q258532": "British America",
    "Q3916279": "Macau",
    "Q23498721": "Gold Coast",
}


def apply_reviewed_crosswalk(project_root: Path) -> None:
    global CHINA_CONTEXT_IDS
    global BRITISH_EMPIRE_CONTEXT_IDS
    global NON_EUROPE_GEOGRAPHIC_IDS
    global COLONIAL_CONTEXT_IDS
    global TRANSCONTINENTAL_OR_IMPERIAL_IDS
    global EUROPE_GEOGRAPHIC_IDS
    global SPECIAL_CONTEXTS
    global TOKEN_LABEL_OVERRIDES

    scope_sets = build_reviewed_scope_sets(
        project_root,
        fallback_china_ids=CHINA_CONTEXT_IDS,
        fallback_british_empire_ids=BRITISH_EMPIRE_CONTEXT_IDS,
        fallback_non_europe_ids=NON_EUROPE_GEOGRAPHIC_IDS,
        fallback_colonial_ids=COLONIAL_CONTEXT_IDS,
        fallback_transcontinental_ids=TRANSCONTINENTAL_OR_IMPERIAL_IDS,
        fallback_europe_ids=EUROPE_GEOGRAPHIC_IDS,
    )
    CHINA_CONTEXT_IDS = scope_sets["china_context_ids"]
    BRITISH_EMPIRE_CONTEXT_IDS = scope_sets["british_empire_context_ids"]
    NON_EUROPE_GEOGRAPHIC_IDS = scope_sets["non_europe_geographic_ids"]
    COLONIAL_CONTEXT_IDS = scope_sets["colonial_context_ids"]
    TRANSCONTINENTAL_OR_IMPERIAL_IDS = scope_sets["transcontinental_or_imperial_ids"]
    EUROPE_GEOGRAPHIC_IDS = scope_sets["europe_geographic_ids"]
    SPECIAL_CONTEXTS = {
        "China/Qing context": CHINA_CONTEXT_IDS,
        "British Empire/colonial context": BRITISH_EMPIRE_CONTEXT_IDS,
    }
    TOKEN_LABEL_OVERRIDES = load_token_label_overrides(project_root, TOKEN_LABEL_OVERRIDES)


def id_label_lookup_for_row(row) -> dict[str, str]:
    lookup = {}
    if pd.notna(row.place_id):
        lookup[str(row.place_id)] = str(row.place_label)
    for id_column, label_column in TOKEN_COLUMNS:
        ids = split_pipe_values(getattr(row, id_column))
        labels = split_pipe_values(getattr(row, label_column))
        lookup.update(dict(zip(ids, labels, strict=False)))
    return lookup


def collect_context_tokens(group: pd.DataFrame) -> tuple[list[str], dict[str, str]]:
    ids = []
    label_lookup = {}
    for row in group.itertuples(index=False):
        row_lookup = id_label_lookup_for_row(row)
        label_lookup.update(row_lookup)
        ids.append(row.place_id)
        for id_column, _label_column in TOKEN_COLUMNS:
            ids.extend(split_pipe_values(getattr(row, id_column)))
    return ordered_unique(ids), label_lookup


def classify_birth_scope(token_ids: set[str]) -> str:
    has_non_europe = bool(token_ids & NON_EUROPE_GEOGRAPHIC_IDS)
    has_colonial = bool(token_ids & COLONIAL_CONTEXT_IDS)
    has_europe = bool(token_ids & EUROPE_GEOGRAPHIC_IDS)
    has_transcontinental = bool(token_ids & TRANSCONTINENTAL_OR_IMPERIAL_IDS)

    if has_non_europe and (has_europe or has_colonial or has_transcontinental):
        return "mixed_europe_non_europe_or_colonial"
    if has_non_europe or has_colonial:
        return "non_europe_or_colonial"
    if has_europe:
        return "europe"
    if has_transcontinental:
        return "transcontinental_or_imperial_context"
    return "unmapped_birth_scope"


def collapse_entity_base(enriched_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for wikidata_id, group in enriched_df.groupby("wikidata_id", dropna=False):
        citizenship_ids = []
        citizenship_labels = []
        for value in group["citizenship_ids"]:
            citizenship_ids.extend(split_pipe_values(value))
        for value in group["citizenship_labels"]:
            citizenship_labels.extend(split_pipe_values(value))

        rows.append(
            {
                "wikidata_id": wikidata_id,
                "person_id": qid_from_uri(wikidata_id),
                "name": group["name"].dropna().iloc[0] if group["name"].notna().any() else pd.NA,
                "source_row_count": len(group),
                "birth_year_values": join_values(group["birth_year"]),
                "birth_place_values": join_values(group["birth_place"]),
                "citizenship_ids": join_values(citizenship_ids),
                "citizenship_labels": join_values(citizenship_labels),
                "has_country_of_citizenship": bool(ordered_unique(citizenship_ids)),
            }
        )

    return pd.DataFrame(rows).sort_values(["name", "wikidata_id"])


def build_birth_scope_table(entity_df: pd.DataFrame, place_df: pd.DataFrame) -> pd.DataFrame:
    birth_df = place_df.loc[place_df["place_role"] == "birth_place"].copy()
    rows = []

    for wikidata_id, group in birth_df.groupby("wikidata_id", dropna=False):
        token_ids, label_lookup = collect_context_tokens(group)
        token_set = set(token_ids)
        scope = classify_birth_scope(token_set)
        non_europe_tokens = token_set & NON_EUROPE_GEOGRAPHIC_IDS
        colonial_tokens = token_set & COLONIAL_CONTEXT_IDS
        europe_tokens = token_set & EUROPE_GEOGRAPHIC_IDS
        trans_tokens = token_set & TRANSCONTINENTAL_OR_IMPERIAL_IDS

        def labels_for(ids: set[str]) -> object:
            labels = [TOKEN_LABEL_OVERRIDES.get(token_id, label_lookup.get(token_id, token_id)) for token_id in ids]
            return join_values(sorted(labels))

        rows.append(
            {
                "wikidata_id": wikidata_id,
                "birth_place_context_row_count": len(group),
                "birth_place_ids": join_values(group["place_id"]),
                "birth_place_labels": join_values(group["place_label"]),
                "birth_scope_category": scope,
                "counts_as_europe_in_binary_rollup": scope == "europe",
                "counts_as_non_europe_or_colonial_in_binary_rollup": scope
                in {"non_europe_or_colonial", "mixed_europe_non_europe_or_colonial"},
                "birth_context_token_ids": join_values(token_ids),
                "birth_context_token_labels": labels_for(token_set),
                "europe_token_ids": join_values(sorted(europe_tokens)),
                "europe_token_labels": labels_for(europe_tokens),
                "non_europe_token_ids": join_values(sorted(non_europe_tokens)),
                "non_europe_token_labels": labels_for(non_europe_tokens),
                "colonial_token_ids": join_values(sorted(colonial_tokens)),
                "colonial_token_labels": labels_for(colonial_tokens),
                "transcontinental_or_imperial_token_ids": join_values(sorted(trans_tokens)),
                "transcontinental_or_imperial_token_labels": labels_for(trans_tokens),
            }
        )

    birth_scope_df = pd.DataFrame(rows)
    merged = entity_df.merge(birth_scope_df, on="wikidata_id", how="left")
    merged["birth_scope_category"] = merged["birth_scope_category"].fillna("no_birth_place_context")
    for column in [
        "counts_as_europe_in_binary_rollup",
        "counts_as_non_europe_or_colonial_in_binary_rollup",
    ]:
        merged[column] = merged[column].fillna(False).astype(bool)
    merged["birth_place_context_row_count"] = merged["birth_place_context_row_count"].fillna(0).astype(int)
    return merged.sort_values(["birth_scope_category", "name", "wikidata_id"])


def has_any_exact_token(value, target_ids: set[str]) -> bool:
    return bool(set(split_pipe_values(value)) & target_ids)


def collect_row_tokens(row) -> tuple[set[str], dict[str, str]]:
    lookup = id_label_lookup_for_row(row)
    tokens = {str(row.place_id)} if pd.notna(row.place_id) else set()
    for id_column, _label_column in TOKEN_COLUMNS:
        tokens.update(split_pipe_values(getattr(row, id_column)))
    return tokens, lookup


def build_special_context_cases(place_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in place_df.itertuples(index=False):
        row_tokens, label_lookup = collect_row_tokens(row)
        for context_name, context_ids in SPECIAL_CONTEXTS.items():
            matched_ids = sorted(row_tokens & context_ids)
            if not matched_ids:
                continue
            matched_labels = [
                TOKEN_LABEL_OVERRIDES.get(token_id, label_lookup.get(token_id, token_id)) for token_id in matched_ids
            ]
            rows.append(
                {
                    "context_family": context_name,
                    "wikidata_id": row.wikidata_id,
                    "person_id": row.person_id,
                    "name": row.name,
                    "place_role": row.place_role,
                    "place_id": row.place_id,
                    "place_label": row.place_label,
                    "matched_token_ids": join_values(matched_ids),
                    "matched_token_labels": join_values(matched_labels),
                    "direct_country_ids": row.direct_country_ids,
                    "direct_country_labels": row.direct_country_labels,
                    "admin_country_ids": row.admin_country_ids,
                    "admin_country_labels": row.admin_country_labels,
                    "context_country_ids": row.context_country_ids,
                    "context_country_labels": row.context_country_labels,
                    "admin_entity_ids": row.admin_entity_ids,
                    "admin_entity_labels": row.admin_entity_labels,
                }
            )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["context_family", "name", "place_role", "place_label"])


def build_summary(entity_df: pd.DataFrame, scope_df: pd.DataFrame, special_df: pd.DataFrame) -> pd.DataFrame:
    total_entities = len(entity_df)
    citizenship_present = int(entity_df["has_country_of_citizenship"].sum())
    citizenship_missing = total_entities - citizenship_present
    citizenship_china = int(entity_df["citizenship_ids"].apply(has_any_exact_token, args=(CHINA_CONTEXT_IDS,)).sum())
    citizenship_british_empire = int(
        entity_df["citizenship_ids"].apply(has_any_exact_token, args=(BRITISH_EMPIRE_CONTEXT_IDS,)).sum()
    )

    if special_df.empty:
        china_context_entities = 0
        british_context_entities = 0
    else:
        china_context_entities = special_df.loc[
            special_df["context_family"] == "China/Qing context", "wikidata_id"
        ].nunique()
        british_context_entities = special_df.loc[
            special_df["context_family"] == "British Empire/colonial context", "wikidata_id"
        ].nunique()

    rows = [
        {
            "metric": "total_entities",
            "entity_count": total_entities,
            "denominator": total_entities,
            "pct": percentage(total_entities, total_entities),
            "notes": "Current pipeline cohort, one row per Wikidata entity.",
        },
        {
            "metric": "country_of_citizenship_present",
            "entity_count": citizenship_present,
            "denominator": total_entities,
            "pct": percentage(citizenship_present, total_entities),
            "notes": "Entities with at least one Wikidata P27 value.",
        },
        {
            "metric": "country_of_citizenship_missing",
            "entity_count": citizenship_missing,
            "denominator": total_entities,
            "pct": percentage(citizenship_missing, total_entities),
            "notes": "Entities without a Wikidata P27 value in the enrichment export.",
        },
        {
            "metric": "citizenship_china_or_chinese_state",
            "entity_count": citizenship_china,
            "denominator": total_entities,
            "pct": percentage(citizenship_china, total_entities),
            "notes": "Exact P27 token match against reviewed China/Qing-related IDs.",
        },
        {
            "metric": "citizenship_british_empire_or_colonial_state",
            "entity_count": citizenship_british_empire,
            "denominator": total_entities,
            "pct": percentage(citizenship_british_empire, total_entities),
            "notes": "Exact P27 token match against reviewed British imperial or colonial IDs.",
        },
        {
            "metric": "place_context_china_or_chinese_state_any_role",
            "entity_count": china_context_entities,
            "denominator": total_entities,
            "pct": percentage(china_context_entities, total_entities),
            "notes": "Any birth/death/residence/work-location row with reviewed China/Qing context.",
        },
        {
            "metric": "place_context_british_empire_or_colonial_state_any_role",
            "entity_count": british_context_entities,
            "denominator": total_entities,
            "pct": percentage(british_context_entities, total_entities),
            "notes": "Any birth/death/residence/work-location row with reviewed British imperial or colonial context.",
        },
    ]

    scope_categories = [
        "europe",
        "non_europe_or_colonial",
        "mixed_europe_non_europe_or_colonial",
        "transcontinental_or_imperial_context",
        "unmapped_birth_scope",
        "no_birth_place_context",
    ]
    for category in scope_categories:
        count = scope_df.loc[scope_df["birth_scope_category"] == category, "wikidata_id"].nunique()
        rows.append(
            {
                "metric": f"birth_scope_{category}",
                "entity_count": int(count),
                "denominator": total_entities,
                "pct": percentage(int(count), total_entities),
                "notes": "Primary geographic scope classification from birth-place context.",
            }
        )

    non_europe_inclusive = int(scope_df["counts_as_non_europe_or_colonial_in_binary_rollup"].sum())
    europe_binary = int(scope_df["counts_as_europe_in_binary_rollup"].sum())
    binary_total = non_europe_inclusive + europe_binary
    rows.extend(
        [
            {
                "metric": "birth_scope_non_europe_or_colonial_inclusive",
                "entity_count": non_europe_inclusive,
                "denominator": total_entities,
                "pct": percentage(non_europe_inclusive, total_entities),
                "notes": "Includes mixed Europe/non-Europe or colonial birth-place contexts.",
            },
            {
                "metric": "birth_scope_europe_binary_rollup",
                "entity_count": europe_binary,
                "denominator": total_entities,
                "pct": percentage(europe_binary, total_entities),
                "notes": "Strict European birth-place context, excluding mixed, colonial, transcontinental, and unmapped.",
            },
            {
                "metric": "birth_scope_binary_classified_total",
                "entity_count": binary_total,
                "denominator": total_entities,
                "pct": percentage(binary_total, total_entities),
                "notes": "Entities classified into the strict Europe vs non-Europe/colonial rollup.",
            },
            {
                "metric": "birth_scope_europe_share_of_binary_classified",
                "entity_count": europe_binary,
                "denominator": binary_total,
                "pct": percentage(europe_binary, binary_total),
                "notes": "Share among entities with binary-classified birth-place context.",
            },
            {
                "metric": "birth_scope_non_europe_or_colonial_share_of_binary_classified",
                "entity_count": non_europe_inclusive,
                "denominator": binary_total,
                "pct": percentage(non_europe_inclusive, binary_total),
                "notes": "Share among entities with binary-classified birth-place context.",
            },
        ]
    )

    return pd.DataFrame(rows)


def parse_args() -> object:
    parser = ArgumentParser(description="Build geographic-scope diagnostics for a cohort.")
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
    apply_reviewed_crosswalk(project_root)
    paths = cohort_paths(project_root, args.cohort_id)
    enriched_path = paths.enriched_path
    place_path = paths.processed_dir / "place_context_long.csv"
    output_dir = paths.processed_dir

    if not enriched_path.exists():
        raise SystemExit(
            f"Missing enriched cohort. Run python scripts/pipeline/04_merge_wikidata_enrichment.py --cohort-id {paths.cohort_id}"
        )
    if not place_path.exists():
        raise SystemExit(
            f"Missing place context. Run python scripts/analysis/02_build_place_affiliation_context.py --cohort-id {paths.cohort_id}"
        )

    enriched_df = normalize_blank_strings(pd.read_csv(enriched_path))
    place_df = normalize_blank_strings(pd.read_csv(place_path))

    entity_df = collapse_entity_base(enriched_df)
    scope_df = build_birth_scope_table(entity_df, place_df)
    special_df = build_special_context_cases(place_df)
    missing_citizenship_df = scope_df.loc[~scope_df["has_country_of_citizenship"]].copy()
    summary_df = build_summary(entity_df, scope_df, special_df)

    output_paths = [
        (output_dir / "geographic_scope_entity_classification.csv", scope_df),
        (output_dir / "geographic_scope_summary.csv", summary_df),
        (output_dir / "geographic_scope_special_context_cases.csv", special_df),
        (
            output_dir / "citizenship_missing_entities.csv",
            missing_citizenship_df[
                [
                    "wikidata_id",
                    "person_id",
                    "name",
                    "birth_year_values",
                    "birth_place_values",
                    "birth_scope_category",
                    "birth_place_labels",
                ]
            ],
        ),
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    for output_path, output_df in output_paths:
        output_df.to_csv(output_path, index=False)

    print("Geographic scope analysis complete.")
    print(f"Cohort: {paths.cohort_id}")
    print(f"Entity rows: {len(entity_df)}")
    print(f"Missing country-of-citizenship rows: {len(missing_citizenship_df)}")
    print(f"Special context rows: {len(special_df)}")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
