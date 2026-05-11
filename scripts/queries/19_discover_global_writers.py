from argparse import ArgumentParser
from io import BytesIO
from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import cohort_paths  # noqa: E402
from wikidata_api import normalize_qid  # noqa: E402


DEFAULT_ENDPOINT = "https://query.wikidata.org/sparql"
DEFAULT_ACCEPT = "text/csv"
USER_AGENT = "Chomputation/0.1 (global writer discovery)"
SCRIPT_REPORTED_COUNT = 18697


def discovery_where(year: int | None = None) -> str:
    year_filter = (
        f"  FILTER(?birthYearValue = {year})"
        if year is not None
        else "  FILTER(?birthYearValue >= 1675 && ?birthYearValue <= 1775)"
    )
    return f"""
WHERE {{
  ?person wdt:P31 wd:Q5;
          wdt:P569 ?birthDate;
          wdt:P106/wdt:P279* wd:Q36180.
  BIND(YEAR(?birthDate) AS ?birthYearValue)
{year_filter}
}}
"""


def count_query(year: int | None = None) -> str:
    return f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT (COUNT(DISTINCT ?person) AS ?distinctEntities)
{discovery_where(year)}
"""


def discovery_query(limit: int, offset: int, year: int | None = None) -> str:
    return f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>

SELECT
  ?person
  ?birthDate
  ?birthYearValue
{discovery_where(year)}
ORDER BY ?person
LIMIT {limit}
OFFSET {offset}
"""


def run_query(query_text: str, endpoint: str) -> bytes:
    encoded_body = urlencode({"query": query_text}).encode("utf-8")
    request = Request(
        url=endpoint,
        data=encoded_body,
        headers={
            "Accept": DEFAULT_ACCEPT,
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )

    for attempt in range(1, 7):
        try:
            with urlopen(request, timeout=180) as response:
                return response.read()
        except HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            if attempt == 6 or error.code not in {429, 500, 502, 503, 504}:
                raise SystemExit(f"Wikidata query failed with HTTP {error.code}.\n{body}") from None
            retry_after = error.headers.get("Retry-After")
            sleep(int(retry_after) if retry_after and retry_after.isdigit() else attempt * 15)
        except URLError as error:
            if attempt == 6:
                raise SystemExit(f"Could not reach Wikidata Query Service: {error}") from None
            sleep(attempt * 15)

    raise RuntimeError("Unreachable query retry state.")


def query_csv(query_text: str, endpoint: str) -> pd.DataFrame:
    response_bytes = run_query(query_text, endpoint)
    return pd.read_csv(BytesIO(response_bytes))


def normalize_discovery_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(column).strip() for column in df.columns]
    if "birthYearValue" not in df.columns and "birthYear" in df.columns:
        df["birthYearValue"] = df["birthYear"]

    for column in [
        "person",
        "personLabel",
        "birthDate",
        "birthYearValue",
        "occupation_ids",
        "occupation_labels",
    ]:
        if column not in df.columns:
            df[column] = pd.NA
    df = df.sort_values(["person", "birthDate"]).drop_duplicates("person")
    df["person_qid"] = df["person"].map(normalize_qid)
    df["birthYear"] = df["birthYearValue"]
    return df[
        [
            "person",
            "person_qid",
            "personLabel",
            "birthDate",
            "birthYear",
            "occupation_ids",
            "occupation_labels",
        ]
    ]


def hydrate_from_enrichment(discovery_df: pd.DataFrame, enrichment_path: Path) -> pd.DataFrame:
    if not enrichment_path.exists():
        return discovery_df

    enrichment_df = pd.read_csv(
        enrichment_path,
        usecols=["person", "personLabel", "occupation_ids", "occupation_labels"],
    ).drop_duplicates("person")
    metadata = enrichment_df.set_index("person")
    hydrated_df = discovery_df.copy()

    for column in ["personLabel", "occupation_ids", "occupation_labels"]:
        lookup = metadata[column].to_dict()
        current = hydrated_df[column] if column in hydrated_df.columns else pd.Series(pd.NA, index=hydrated_df.index)
        hydrated_df[column] = current.where(
            current.notna() & (current.astype(str).str.strip() != ""),
            hydrated_df["person"].map(lookup),
        )

    return hydrated_df


def parse_args() -> object:
    parser = ArgumentParser(description="Discover the global 1675-1775 writer/subclass Wikidata cohort.")
    parser.add_argument("--limit", type=int, default=5000, help="SPARQL page size. Default: 5000.")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional page cap for smoke tests.")
    parser.add_argument("--start-year", type=int, default=1675, help="Start birth year. Default: 1675.")
    parser.add_argument("--end-year", type=int, default=1775, help="End birth year. Default: 1775.")
    parser.add_argument(
        "--broad-count",
        action="store_true",
        help="Also run the broad COUNT(DISTINCT ?person) query. This can be slower than the year-chunked export.",
    )
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help=f"SPARQL endpoint. Default: {DEFAULT_ENDPOINT}")
    parser.add_argument("--output", default=None, help="Output CSV. Default: global_writers cohort discovery path.")
    parser.add_argument(
        "--hydrate-only",
        action="store_true",
        help="Skip WDQS and fill labels/occupation fields in the existing discovery CSV from the API enrichment CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, "global_writers")
    output_path = project_root / args.output if args.output else paths.raw_discovery_path
    summary_path = output_path.with_name(output_path.stem + "_summary.csv")

    expected_count = pd.NA
    if args.hydrate_only:
        if not output_path.exists():
            raise SystemExit(f"Cannot hydrate missing discovery file: {output_path}")
        discovery_df = normalize_discovery_df(pd.read_csv(output_path))
    else:
        if args.broad_count:
            count_df = query_csv(count_query(), args.endpoint)
            expected_count = int(count_df["distinctEntities"].iloc[0])

        frames = []
        total_pages = 0
        for year in range(args.start_year, args.end_year + 1):
            offset = 0
            page_index = 0
            while True:
                if args.max_pages is not None and total_pages >= args.max_pages:
                    break
                page_df = query_csv(discovery_query(args.limit, offset, year), args.endpoint)
                if page_df.empty:
                    break
                frames.append(page_df)
                total_pages += 1
                print(f"Fetched {year} page {page_index + 1}: {len(page_df)} rows at OFFSET {offset}")
                if len(page_df) < args.limit:
                    break
                page_index += 1
                offset += args.limit
            if args.max_pages is not None and total_pages >= args.max_pages:
                break

        discovery_df = normalize_discovery_df(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())

    discovery_df = hydrate_from_enrichment(discovery_df, paths.raw_enrichment_path)
    if pd.isna(expected_count):
        expected_count = int(discovery_df["person"].nunique())

    output_path.parent.mkdir(parents=True, exist_ok=True)
    discovery_df.to_csv(output_path, index=False)

    summary_df = pd.DataFrame(
        [
            {
                "metric": "script_reported_global_count",
                "value": SCRIPT_REPORTED_COUNT,
                "notes": "Count reported in Conference Paper Script.docx.",
            },
            {
                "metric": "current_wdqs_count",
                "value": expected_count,
                "notes": (
                    "COUNT(DISTINCT ?person) from the broad discovery query."
                    if args.broad_count
                    else "Distinct exported entities from the year-chunked discovery queries."
                ),
            },
            {
                "metric": "exported_distinct_entities",
                "value": int(discovery_df["person"].nunique()),
                "notes": "Distinct entities written to the discovery CSV.",
            },
            {
                "metric": "count_delta_current_minus_script",
                "value": expected_count - SCRIPT_REPORTED_COUNT,
                "notes": "Positive means current Wikidata returns more entities than the script count.",
            },
        ]
    )
    summary_df.to_csv(summary_path, index=False)

    print("Global writer discovery complete.")
    print(f"WDQS count: {expected_count}")
    print(f"Script reported count: {SCRIPT_REPORTED_COUNT}")
    print(f"Exported entities: {discovery_df['person'].nunique()}")
    print(f"Output CSV: {output_path}")
    print(f"Summary CSV: {summary_path}")


if __name__ == "__main__":
    main()
