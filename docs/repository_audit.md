# Repository Audit

Last updated: 2026-05-11.

This note records the cleanup state after the French-seed / global-writer
bridge and the first granular occupation-bucket layer. It is meant as a quick
handoff map: what is active, what is intentionally retained, and what should
not be cleaned automatically.

## Inventory Snapshot

| Item | Count or size | Note |
|---|---:|---|
| Tracked files before this audit cleanup | 130 | `git ls-files` count before adding this note and `scripts/common.py` |
| Tracked processed CSV artifacts | 64 | Current committed research outputs under `data/processed/` |
| Local data footprint excluding API cache | 148.86 MB | Includes ignored `data/interim/` working tables |
| Ignored API cache footprint | 1.85 GB | `data/raw/cache/`, useful for resumable reruns |

Largest non-cache data files at audit time:

| Path | Size |
|---|---:|
| `data/processed/global_writers/wikipedia_representation_long.csv` | 28.26 MB |
| `data/processed/global_writers/place_context_long.csv` | 17.31 MB |
| `data/raw/global_writers/wikidata_person_place_context.csv` | 16.05 MB |
| `data/interim/global_writers/writers_wikidata_enriched.csv` | 10.78 MB |
| `data/processed/global_writers/cultural_affiliation_evidence_matrix.csv` | 8.32 MB |
| `data/processed/global_writers/occupation_bucket_entities_long.csv` | 8.31 MB |
| `data/processed/global_writers/representation_entities.csv` | 7.03 MB |
| `data/raw/global_writers/wikidata_affiliation_enrichment.csv` | 6.08 MB |

## Cleanup Completed

- Centralized shared script constants and deterministic helper functions in
  `scripts/common.py`.
- Centralized cohort IDs in `scripts/cohorts.py`.
- Refactored active pipeline and analysis scripts to reuse those helpers
  instead of carrying duplicate `split_pipe_values`, `percentage`, `ratio`,
  `qid_from_uri`, `WIKI_COLUMNS`, and language-code definitions.
- Regenerated affected analysis layers for both cohorts and confirmed no
  tracked data outputs changed.
- Updated documentation so occupation buckets are the implemented Analysis
  Layer 06 and map/network work is a later visualization layer.
- Clarified that `outputs/` is ignored scratch space, while active tables live
  under `data/processed/`.

## Active Structure

The active project shape is:

- `data/cohorts/`: cohort manifest.
- `data/reference/`: reviewable interpretive crosswalks.
- `data/raw/`: source exports and API-generated raw enrichment files.
- `data/interim/`: ignored reproducible working tables.
- `data/processed/`: versioned analysis-ready outputs and comparison tables.
- `scripts/pipeline/`: deterministic merge, clean, diagnose, and enrichment
  merge steps.
- `scripts/queries/`: source acquisition and Wikidata API helpers.
- `scripts/analysis/`: cohort-level analysis layers.
- `scripts/common.py`: shared constants and small pure helpers.

The active cohorts remain:

- `french_seed`: 1,638 distinct Wikidata people, retained as legacy/provenance.
- `global_writers`: 14,377 distinct Wikidata people, the active reproducible
  discovery spine.

The legacy French seed is redundant as discovery data: all 1,638 of its
distinct QIDs are present in `global_writers`. New France/Germany/British/
China comparisons should use the reproducible context-slice tables generated
from the global cohort:

- `data/processed/global_writers/context_slice_membership.csv`
- `data/processed/global_writers/context_slice_summary.csv`
- `data/processed/french_seed_redundancy_audit.csv`

## Intentional Retention

The following are not cleanup targets right now:

- `data/raw/cache/`: large, ignored, and useful while API queries still change.
- Legacy SPARQL scripts in `scripts/queries/`: no longer the recommended
  full-cohort route, but useful for provenance and small checks.
- Legacy French-seed raw CSVs: retained so previous work can be audited, but
  not needed for new discovery.
- Versioned processed CSVs: currently useful for collaborator handoff and
  conference-facing reproducibility.
- Reference crosswalk seed files: intentionally reviewable, even when some
  rows are marked `needs_review`.

## Remaining Decisions

- Whether to move more hard-coded historical/geographic QID sets out of Python
  and into reference tables.
- Which occupation buckets should be manually curated for presentation.
- How to set high/medium/low thresholds for the formula-backed affiliation
  evidence scores.
- Which language editions should appear in final visualizations.
- When to add BnF as the first external comparison source.
