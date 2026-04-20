# Raw Data

This folder holds source exports. Pipeline scripts may read these files, but
they should not mutate them in place.

Current raw inputs:

- `18thcentury_french_writers_table.csv`
- `18thcentury_writers_wikidata_viaf.csv`

Current provenance note:

- These two pilot raw CSVs were manually exported from the Wikidata Query
  Service by a project collaborator.
- The exact original query texts were not preserved in the repository.
- The repository now includes reconstructed current-cohort query templates and
  a query runner so future exports can be reproduced more transparently.

Expected Step 04 export:

- `wikidata_affiliation_enrichment.csv`
- or one or more family-specific CSV folders such as:
  - `wikidata_affiliation_death_place_parts/`
  - `wikidata_affiliation_residence_parts/`
  - `wikidata_affiliation_work_location_parts/`
  - `wikidata_affiliation_languages_parts/`
  - `wikidata_affiliation_citizenship_parts/`
  - `wikidata_wikipedia_representation_parts/`

Create the Step 04 export by running the generated Wikidata enrichment queries
in the Wikidata Query Service and exporting the results as CSV. For the full
current cohort, the recommended workflow is the split query-family approach,
not the single all-fields query. If queries are run in chunks, either combine
the CSV files into the default export above or pass one or more chunk folders
to `scripts/pipeline/04_merge_wikidata_enrichment.py` with `--input`.

Reconstructed current-cohort Wikidata queries:

- `scripts/queries/03_wikidata_current_cohort_geography_export.rq`
- `scripts/queries/04_wikidata_current_cohort_viaf_export.rq`
- `scripts/queries/00_run_wikidata_sparql_query.py`

These reconstruction queries are intended to re-export the current pilot cohort
from stored QIDs. They improve forward reproducibility, but they should not be
described as the exact original discovery queries unless those originals are
later recovered.
