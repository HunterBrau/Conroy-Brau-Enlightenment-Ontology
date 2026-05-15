# Conference Script Alignment

Inspected source document:

```text
C:/Users/hbrau/Downloads/Conference Paper Script.docx
```

Inspection date: 2026-05-04.

This note compares the latest partner-authored conference script with the
current repository pipeline. It is meant to prevent a corpus-scope mismatch
from being hidden inside later analysis.

## What The Script Says

The script frames the project around three research questions:

- how European eighteenth-century writers are represented in Wikidata and
  Wikipedia;
- whether Wikidata or Wikipedia language affects which biographies are
  represented;
- whether evidence fields such as citizenship and place context change the
  resulting national or imperial picture.

The script's "Global Picture" section describes a broad Wikidata cohort:

```text
people with occupation writer or a subclass of writer
born between 1675 and 1775
grouped by country of citizenship
```

It reports 18,697 people in that global writer/subclass cohort and gives
country-of-citizenship counts such as France, Germany, the Russian Empire,
Spain, the Ottoman Empire, the United States, Japan, Mexico, India, Canada,
Australia, China, and South Korea.

The script also includes comments that are important provenance evidence:

- the first query screenshot is "only for writers of French nationality";
- the map screenshot is "only for writers of French nationality";
- 1675-1775 is confirmed as the date range;
- British Empire/Britain/England/Scotland/United Kingdom are called out as a
  data issue that may require joined or comparative queries.

The first screenshot shows a SPARQL query with:

```sparql
?writer wdt:P31 wd:Q5;
        wdt:P27 wd:Q142;
        wdt:P569 ?birthDate;
        wdt:P106 ?occupation.

FILTER(YEAR(?birthDate) >= 1675 && YEAR(?birthDate) <= 1775)
?occupation wdt:P279* wd:Q36180.
```

That is a France country-of-citizenship query, not a global writer query.

## What The Repo Now Contains

The repository now has two explicit cohort tracks.

The `french_seed` track preserves the original local workflow. It starts from
two raw CSVs:

- `data/raw/18thcentury_french_writers_table.csv`
- `data/raw/18thcentury_writers_wikidata_viaf.csv`

Follow-up audit: the original French seed is now best treated as provenance,
not active discovery. All 1,638 distinct QIDs from the legacy seed are present
in the reproducible `global_writers` cohort. New France-facing comparisons
should use the context-slice outputs generated from `global_writers`:

- `data/processed/global_writers/context_slice_membership.csv`
- `data/processed/global_writers/context_slice_summary.csv`
- `data/processed/french_seed_redundancy_audit.csv`

The first raw CSV is the base cohort. It has:

```text
person
personLabel
birthYear
birthPlaceLabel
coords
occupations
```

It currently contains 1,722 rows and 1,638 distinct Wikidata entities.

The VIAF CSV is merged onto that base cohort after the fact. It provides VIAF
IDs and birth-date evidence. It is not the primary discovery source for the
cohort.

The `global_writers` track adds the missing global layer assumed by parts of
the conference script:

- `data/raw/global_writers_1675_1775_discovery.csv`
- `data/interim/global_writers/`
- `data/processed/global_writers/`

Both tracks now use the same Wikidata API enrichment route and equivalent
analysis scripts.

## Original Gap

The repo originally supported a manual French-facing pilot cohort, while the
script also made claims about a global Wikidata writer/subclass cohort.

Those are different corpora:

```text
Legacy French-facing seed:
manual export of people born 1675-1775 with writer/subclass occupation and
French-facing selection criteria that were not preserved as a script

Global writer/subclass cohort:
people born 1675-1775 with writer/subclass occupation, regardless of citizenship
```

This has now been bridged by adding `global_writers`, but the distinction still
matters: outputs from `french_seed` should not be presented as global
proportions of eighteenth-century writers.

## Count Mismatch To Resolve

The script reports 18,697 people in the global writer/subclass citizenship
table, but later text near the map says there are 14,337 writers, poets, and
playwrights on that map. The implemented global discovery query currently
returns 14,377 distinct Wikidata entities.

That means the current reproducible discovery output is much closer to the
script's map language than to the earlier 18,697 table count.

The script also reports 1,552 French national writers in the global citizenship
table. The current repo French-seed raw cohort has 1,638 distinct entities.

The current API enrichment finds 1,327 of those 1,638 entities with exact
current `country of citizenship = France (Q142)`.

That mismatch can have several causes:

- the raw map/geography export may have used a related but not identical query;
- Wikidata statements may have changed since the script screenshots were made;
- the script table may count exact P27 values differently from the map export;
- duplicate labels, duplicate entity rows, or required coordinate/birth-place
  fields may have changed row counts;
- historical states such as Kingdom of France may be represented separately
  from modern France in current Wikidata.

The original exact export query text is not preserved in the repo, so this
cannot be resolved from local files alone.

Current implemented bridge outputs:

- `data/raw/global_writers_1675_1775_discovery.csv`
- `data/raw/global_writers_1675_1775_discovery_summary.csv`
- `data/interim/global_writers/`
- `data/processed/global_writers/`
- `data/processed/cohort_comparison_summary.csv`
- `data/processed/cohort_comparison_country_citizenship.csv`
- `data/processed/cohort_comparison_geographic_scope.csv`
- `data/processed/cohort_comparison_language_representation.csv`

## Smaller Data Issues In The Script

The script table labels China as `Q48`; China is `Q148` in Wikidata. `Q48` is
Asia. This should be corrected before producing query templates or final
tables from that text.

The script also uses country of citizenship as the main national grouping, but
the British Empire discussion already shows why this field is historically
fragile. British imperial, colonial, dynastic, and successor-state contexts
need a multi-signal analysis rather than one exact P27 query.

## Implemented Bridge

1. The legacy corpus is labeled as `french_seed`, the earlier French-facing
   pilot.

2. Its current analysis outputs are preserved for backward compatibility, but
   new France-facing claims should use global-derived context slices.

3. A separate discovery layer exists for the global 1675-1775 writer/subclass
   cohort:

```sparql
?person wdt:P31 wd:Q5;
        wdt:P569 ?birthDate;
        wdt:P106 ?occupation.
?occupation wdt:P279* wd:Q36180.
FILTER(YEAR(?birthDate) >= 1675 && YEAR(?birthDate) <= 1775)
```

4. For any paginated SPARQL discovery export, use a stable `ORDER BY ?person`
   before `LIMIT` and `OFFSET`.

5. Enrichment for both cohorts is fetched through the same Wikidata API route.

6. The two parallel analysis tracks are:

```text
global_writers
french_seed
```

7. The China/British Empire diagnostics now run on the global cohort and the
   legacy French seed, using:

```text
country of citizenship
birth place context
death place context
residence
work location
spoken/written language
writing language
```

8. Keep BnF out of the current phase; the scoped project now focuses on the
   reproducible Wikidata/Wikipedia evidence already in the repository.

## Bottom Line

The repo now has the two corpus layers the script needs: a preserved French
seed cohort and a global writer/subclass cohort born 1675-1775. The next
interpretive work should compare those tracks rather than blending them.
