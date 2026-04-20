from argparse import ArgumentParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_ENDPOINT = "https://query.wikidata.org/sparql"
DEFAULT_ACCEPT = "text/csv"
USER_AGENT = "Chomputation/0.1 (Wikidata export helper)"


def parse_args() -> object:
    parser = ArgumentParser(
        description="Run a Wikidata SPARQL query file and save the CSV result."
    )
    parser.add_argument(
        "query_path",
        help="Path to a .rq SPARQL query file.",
    )
    parser.add_argument(
        "output_path",
        help="Path to the output CSV file to write.",
    )
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help=f"SPARQL endpoint URL. Default: {DEFAULT_ENDPOINT}",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    query_path = Path(args.query_path).resolve()
    output_path = Path(args.output_path).resolve()

    if not query_path.exists():
        raise SystemExit(f"Missing query file: {query_path}")

    query_text = query_path.read_text(encoding="utf-8")
    encoded_body = urlencode({"query": query_text}).encode("utf-8")
    request = Request(
        url=args.endpoint,
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
        raise SystemExit(
            f"Wikidata query failed with HTTP {error.code}.\n{error_body}"
        ) from None
    except URLError as error:
        raise SystemExit(f"Could not reach the Wikidata endpoint: {error}") from None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response_bytes)

    print(f"Query file: {query_path}")
    print(f"Output CSV: {output_path}")
    print(f"Bytes written: {len(response_bytes)}")


if __name__ == "__main__":
    main()
