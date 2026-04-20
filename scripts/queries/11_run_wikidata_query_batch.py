from argparse import ArgumentParser
from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_ENDPOINT = "https://query.wikidata.org/sparql"
DEFAULT_ACCEPT = "text/csv"
USER_AGENT = "Chomputation/0.1 (Wikidata batch export helper)"


def parse_args() -> object:
    parser = ArgumentParser(
        description=(
            "Run one generated Wikidata SPARQL query file, or a directory of "
            "query files, and save CSV exports."
        )
    )
    parser.add_argument(
        "query_input",
        help="Path to a .rq file or a directory containing .rq files.",
    )
    parser.add_argument(
        "output_path",
        help=(
            "Path to an output .csv file when query_input is a file, or a "
            "directory to hold CSV exports when query_input is a directory."
        ),
    )
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help=f"SPARQL endpoint URL. Default: {DEFAULT_ENDPOINT}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of query files to run from a directory.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip output CSV files that already exist.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue through the remaining query files if one export fails.",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.0,
        help="Optional pause between directory exports. Default: 0.0",
    )
    return parser.parse_args()


def run_query(query_path: Path, output_path: Path, endpoint: str) -> int:
    query_text = query_path.read_text(encoding="utf-8")
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

    try:
        with urlopen(request, timeout=120) as response:
            response_bytes = response.read()
    except HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Wikidata query failed with HTTP {error.code} for {query_path}.\n"
            f"{error_body}"
        ) from None
    except URLError as error:
        raise RuntimeError(
            f"Could not reach the Wikidata endpoint for {query_path}: {error}"
        ) from None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response_bytes)
    return len(response_bytes)


def list_query_files(query_input: Path, limit: int | None) -> list[Path]:
    if query_input.is_file():
        if query_input.suffix.lower() != ".rq":
            raise SystemExit(f"Expected a .rq file, got: {query_input}")
        return [query_input]

    if query_input.is_dir():
        query_files = sorted(query_input.glob("*.rq"))
        if not query_files:
            raise SystemExit(f"No .rq files found in: {query_input}")
        if limit is not None:
            return query_files[:limit]
        return query_files

    raise SystemExit(f"Missing query input path: {query_input}")


def resolve_output_path(query_input: Path, output_root: Path, query_path: Path) -> Path:
    if query_input.is_file():
        return output_root
    return output_root / f"{query_path.stem}.csv"


def main() -> None:
    args = parse_args()

    query_input = Path(args.query_input).resolve()
    output_root = Path(args.output_path).resolve()
    query_files = list_query_files(query_input, args.limit)

    if query_input.is_file() and output_root.suffix.lower() != ".csv":
        raise SystemExit("When query_input is a file, output_path must be a .csv path.")

    if query_input.is_dir():
        output_root.mkdir(parents=True, exist_ok=True)

    successes = 0
    skips = 0
    failures = 0

    for index, query_path in enumerate(query_files, start=1):
        output_path = resolve_output_path(query_input, output_root, query_path)

        if args.skip_existing and output_path.exists():
            skips += 1
            print(
                f"[{index}/{len(query_files)}] Skipping existing export: {output_path}"
            )
            continue

        try:
            byte_count = run_query(
                query_path=query_path,
                output_path=output_path,
                endpoint=args.endpoint,
            )
        except RuntimeError as error:
            failures += 1
            print(f"[{index}/{len(query_files)}] {error}")
            if not args.continue_on_error:
                raise SystemExit(1) from None
            continue

        successes += 1
        print(
            f"[{index}/{len(query_files)}] Exported {query_path.name} -> "
            f"{output_path.name} ({byte_count} bytes)"
        )

        if args.pause_seconds > 0 and index < len(query_files):
            sleep(args.pause_seconds)

    print()
    print(f"Query files considered: {len(query_files)}")
    print(f"Exports written: {successes}")
    print(f"Exports skipped: {skips}")
    print(f"Exports failed: {failures}")


if __name__ == "__main__":
    main()
