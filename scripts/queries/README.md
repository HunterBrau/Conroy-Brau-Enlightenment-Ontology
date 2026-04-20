# Query Scripts

This folder is for source-acquisition helpers and saved query artifacts.

Planned order of work:

1. Add a richer Wikidata enrichment query or export workflow for affiliation and
   representation fields.
2. Add a BnF comparison query or acquisition workflow.
3. Consider domain-specific authority queries only after the Wikidata-centered
   cohort and BnF comparison are stable.

Current query artifacts:

- `00_run_wikidata_sparql_query.py` runs a `.rq` file against the Wikidata
  Query Service and saves the CSV result.
- `01_export_wikidata_values_block.py` generates a `VALUES ?person` block from
  the cleaned cohort, full current-cohort query files, and chunked query files.
- `11_run_wikidata_query_batch.py` runs one generated query file or a whole
  directory of generated query files and saves matching CSV exports.
- `outputs/wikidata_cohort_values_block.txt` stores the generated block.
- `outputs/wikidata_affiliation_enrichment_query.rq` stores the full generated
  all-in-one Step 04 query.
- `outputs/wikidata_affiliation_queries/` stores chunked query files for the
  all-in-one Step 04 query.
- `outputs/wikidata_affiliation_death_place_query.rq` stores the recommended
  split Step 04 query for death-place evidence.
- `outputs/wikidata_affiliation_death_place_queries/` stores chunked
  death-place queries.
- `outputs/wikidata_affiliation_residence_query.rq` stores the recommended
  split Step 04 query for residence evidence.
- `outputs/wikidata_affiliation_residence_queries/` stores chunked residence
  queries.
- `outputs/wikidata_affiliation_work_location_query.rq` stores the recommended
  split Step 04 query for work-location evidence.
- `outputs/wikidata_affiliation_work_location_queries/` stores chunked
  work-location queries.
- `outputs/wikidata_affiliation_languages_query.rq` stores the recommended
  split Step 04 query for language evidence.
- `outputs/wikidata_affiliation_languages_queries/` stores chunked language
  queries.
- `outputs/wikidata_affiliation_citizenship_query.rq` stores the recommended
  split Step 04 query for citizenship evidence.
- `outputs/wikidata_affiliation_citizenship_queries/` stores chunked
  citizenship queries.
- `outputs/wikidata_wikipedia_representation_query.rq` stores the recommended
  split Step 04 query for Wikipedia representation evidence.
- `outputs/wikidata_wikipedia_representation_queries/` stores chunked
  Wikipedia representation queries.
- `outputs/wikidata_current_cohort_geography_query.rq` stores a reconstructed
  current-cohort geography export query.
- `outputs/wikidata_current_cohort_geography_queries/` stores chunked
  reconstructed geography export queries.
- `outputs/wikidata_current_cohort_viaf_query.rq` stores a reconstructed
  current-cohort VIAF export query.
- `outputs/wikidata_current_cohort_viaf_queries/` stores chunked reconstructed
  VIAF export queries.
- `02_wikidata_affiliation_enrichment.rq` is the Step 04 enrichment template
  for affiliation and representation fields in one combined query.
- `05_wikidata_affiliation_death_place.rq` is the recommended Step 04 split
  template for death-place evidence.
- `06_wikidata_affiliation_languages.rq` is the recommended Step 04 split
  template for language evidence.
- `07_wikidata_wikipedia_representation.rq` is the recommended Step 04 split
  template for Wikipedia representation evidence.
- `08_wikidata_affiliation_residence.rq` is the recommended Step 04 split
  template for residence evidence.
- `09_wikidata_affiliation_work_location.rq` is the recommended Step 04 split
  template for work-location evidence.
- `10_wikidata_affiliation_citizenship.rq` is the recommended Step 04 split
  template for citizenship evidence.
- `03_wikidata_current_cohort_geography_export.rq` is a reconstructed template
  for re-exporting the current pilot cohort's geography fields.
- `04_wikidata_current_cohort_viaf_export.rq` is a reconstructed template for
  re-exporting the current pilot cohort's birth-date and VIAF fields.

The recommended Step 04 workflow for the full cohort is to run the split query
families rather than the single combined enrichment query. The combined query
is still useful for small subsets or inspection, but the split families are
more reliable because they avoid large cross-products between multivalued
properties.

One practical compromise is intentional: the death-place split export captures
death-place IDs and labels, but not death-place coordinates. That keeps the
query reliable on the current cohort. If a later step truly needs death-place
coordinates, they can be queried in a second pass from the death-place entity
IDs.

After running the query or chunked queries, either save a combined CSV as
`data/raw/wikidata_affiliation_enrichment.csv` or pass one or more export paths
or export folders to `scripts/pipeline/04_merge_wikidata_enrichment.py
--input`.

Example batch export:

```powershell
python scripts/queries/11_run_wikidata_query_batch.py `
  outputs/wikidata_affiliation_languages_queries `
  data/raw/wikidata_affiliation_languages_parts `
  --skip-existing
```

For the two existing pilot raw inputs, the exact original collaborator queries
were not preserved. The reconstructed current-cohort export templates are
therefore reproducibility aids, not claims of byte-identical recovery.

The project should avoid introducing several unrelated external acquisition
scripts at once. Wikidata remains the identity spine unless a later research
decision explicitly changes that.
