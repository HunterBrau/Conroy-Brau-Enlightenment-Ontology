# Analysis Scripts

Analysis scripts consume cleaned or enriched pipeline outputs and write
interpretation-ready tables. They should not mutate raw data or interim inputs.

## 00_build_crosswalk_review_matrix.py

Builds review tables for `data/reference/political_entity_affiliation_crosswalk_seed.csv`.

Outputs are written to `data/processed/`:

- `political_entity_crosswalk_review_matrix.csv`
- `political_entity_crosswalk_review_summary.csv`

The matrix keeps manual review labels separate from calculated evidence. It
adds punchcard-style flags for direct state evidence, imperial context,
modern-country rollup, Europe/non-Europe classification, review notes, and
active evidence in the current French/global cohorts. Its `review_tally_score`
is calculated as:

```text
review_tally_checked_count / review_tally_total_fields
```

Run it after place context and enrichment outputs exist:

```powershell
python scripts/analysis/00_build_crosswalk_review_matrix.py
```

## 01_build_representation_matrices.py

Builds the first analysis layer from:

```text
data/interim/writers_wikidata_enriched.csv
```

Outputs are written to `data/processed/` and include:

- one entity-level table
- cultural-affiliation candidate and best-candidate tables
- an unmapped-token audit table
- one row per entity per Wikipedia language edition
- summaries by language edition, gender, affiliation, place-derived
  affiliation, occupation, and Wikidata label coverage

Run it after Step 04, and after `02_build_place_affiliation_context.py` when
place-derived affiliation context should be joined into the representation
tables:

```powershell
python scripts/analysis/01_build_representation_matrices.py
python scripts/analysis/01_build_representation_matrices.py --cohort-id global_writers
```

Reviewed political-entity mappings are loaded from
`data/reference/political_entity_affiliation_crosswalk_seed.csv` and overlaid
on the broader fallback mapping.

## 02_build_place_affiliation_context.py

Builds a separate place-derived evidence layer from:

```text
data/raw/wikidata_person_place_context.csv
```

Outputs are written to `data/processed/` and include:

- one row per person/place/role
- place-derived affiliation candidate and best-candidate tables
- an unmapped country-token audit table
- coverage by place role

Run it after fetching place context:

```powershell
python scripts/queries/18_fetch_wikidata_person_place_context.py
python scripts/analysis/02_build_place_affiliation_context.py
```

## 03_build_geographic_scope_analysis.py

Builds geography-facing diagnostics from:

```text
data/interim/writers_wikidata_enriched.csv
data/processed/place_context_long.csv
```

Outputs are written to `data/processed/` and include:

- one entity-level birth-scope classification table
- a summary table for missing citizenship, China/Qing context, British
  imperial context, and Europe/non-Europe rollups
- a special-context case table showing the place rows that recover China/Qing
  or British imperial evidence
- a list of entities missing country-of-citizenship evidence

Run it after Step 04 and after the place-context layer:

```powershell
python scripts/analysis/03_build_geographic_scope_analysis.py
python scripts/analysis/03_build_geographic_scope_analysis.py --cohort-id global_writers
```

## 04_compare_cohorts.py

Builds comparison tables across the French seed and global writer cohorts.

Outputs are written to `data/processed/` and include:

- high-level summary rows from diagnostics, enrichment, and geographic scope
- country-of-citizenship counts by cohort
- geographic-scope summaries by cohort
- Wikipedia language-edition representation by cohort

Run it after both cohort tracks have processed outputs:

```powershell
python scripts/analysis/04_compare_cohorts.py
```

## 05_build_affiliation_evidence_matrix.py

Builds a formula-backed cultural-affiliation evidence matrix for a cohort.
This is the current table to use when the project needs a transparent
affiliation-strength calculation.

Default evidence fields:

- citizenship
- native language
- spoken/written language
- writing language
- birth place
- death place
- residence
- work location

Outputs are written to the cohort's processed directory:

- `cultural_affiliation_evidence_matrix.csv`
- `cultural_affiliation_evidence_best.csv`
- `cultural_affiliation_evidence_summary.csv`

Scores are pure tallies:

```text
score_over_total_fields = supporting_evidence_count / total_evidence_fields
score_over_available_fields = supporting_evidence_count / available_mapped_evidence_count
```

Run it after Step 04 and the place-context layer:

```powershell
python scripts/analysis/05_build_affiliation_evidence_matrix.py
python scripts/analysis/05_build_affiliation_evidence_matrix.py --cohort-id global_writers
```

## 06_build_occupation_bucket_tables.py

Builds the granular occupation-bucket layer from the raw Wikidata API
enrichment exports. The script uses occupation QIDs as the stable key and
writes a reviewable crosswalk plus cohort-specific bucket summaries.

Reference output:

- `data/reference/occupation_bucket_crosswalk_seed.csv`

Processed outputs:

- `occupation_bucket_crosswalk_summary.csv`
- `occupation_bucket_entities_long.csv`
- `occupation_bucket_summary.csv`
- `occupation_bucket_language_representation.csv`

The current granular buckets include Religion / Theology as a first-class
category, alongside literature, philosophy, science, medicine, politics, law,
education, visual arts, performance, print culture, travel/geography, military,
commerce, social reform, sociability/patronage, engineering, and craft/trade.

Run it after API enrichment and representation outputs exist:

```powershell
python scripts/analysis/06_build_occupation_bucket_tables.py
```

## 07_build_context_slice_tables.py

Builds reproducible France, Germany, British, and China/Qing context slices
from the global writer cohort and the reviewed political-entity crosswalk.
This is the replacement layer for new France-facing contrast claims; the
manual French seed remains provenance only.

Outputs:

- `data/processed/global_writers/context_slice_membership.csv`
- `data/processed/global_writers/context_slice_summary.csv`
- `data/processed/french_seed_redundancy_audit.csv`

The audit table records that all 1,638 distinct legacy French-seed QIDs are
already present in `global_writers`, while the old seed is not equivalent to
current exact France/Q142 citizenship.

Run it after the global place-context and political crosswalk layers exist:

```powershell
python scripts/analysis/07_build_context_slice_tables.py
```
