# Core Findings Packet

Generated from the active `global_writers` analytical spine and the reviewed
context-slice layer. BnF and other external authority sources are out of scope
for the current project phase.

## TL;DR

- The active corpus contains 14,377 Wikidata humans born 1675-1775
  with occupation `writer` or a subclass of writer.
- Country of citizenship is missing for 5,302
  entities (36.88%), so it cannot carry the historical
  argument by itself.
- Context slices are now comparable because France, Germany, British, and
  China/Qing are all derived from the same global cohort and the same reviewed
  political-entity crosswalk.
- Place context materially changes the British and France/Germany slices:
  British has 645 place-only entities, France has 1,258, and Germany has 2,343.
  China/Qing is the contrast case: it is mostly recovered through citizenship
  evidence in the current Wikidata data.
- The top Wikipedia language editions by article coverage are enwiki, dewiki,
  frwiki, ruwiki, and itwiki.
- Occupation evidence is not just "writer": Religion / Theology and Education /
  Scholarship / Humanities are both large enough to support conference-facing
  claims about intellectual networks.

## Context Slices

| slice | any evidence | share of global | citizenship | place context | place-only |
| --- | --- | --- | --- | --- | --- |
| Germany | 3711 | 25.81 | 1368 | 3655 | 2343 |
| France | 2922 | 20.32 | 1664 | 2713 | 1258 |
| British | 2099 | 14.6 | 1454 | 1570 | 645 |
| China | 424 | 2.95 | 404 | 104 | 20 |

## Language Representation

| language_edition | represented_entities | representation_pct |
| --- | --- | --- |
| enwiki | 5305 | 36.9 |
| dewiki | 3980 | 27.68 |
| frwiki | 3481 | 24.21 |
| ruwiki | 2436 | 16.94 |
| itwiki | 2049 | 14.25 |

Language coverage by slice is written to
`data/processed/global_writers/core_findings_language_by_slice.csv`. Use it for
heatmaps or punchcards comparing how each Wikipedia language edition represents
France, Germany, British, and China/Qing contexts.

## Gender

| slice_id | gender_category | slice_entities | slice_pct | global_pct |
| --- | --- | --- | --- | --- |
| british | male | 1679 | 79.99 | 86.98 |
| british | female | 418 | 19.91 | 12.12 |
| british | multiple_or_ambiguous | 1 | 0.05 | 0.01 |
| british | unknown | 1 | 0.05 | 0.89 |
| china | male | 242 | 57.08 | 86.98 |
| china | female | 168 | 39.62 | 12.12 |
| china | unknown | 14 | 3.3 | 0.89 |
| china | multiple_or_ambiguous | 0 | 0 | 0.01 |
| france | male | 2544 | 87.06 | 86.98 |
| france | female | 372 | 12.73 | 12.12 |
| france | unknown | 5 | 0.17 | 0.89 |
| france | multiple_or_ambiguous | 1 | 0.03 | 0.01 |
| germany | male | 3442 | 92.75 | 86.98 |
| germany | female | 260 | 7.01 | 12.12 |
| germany | unknown | 9 | 0.24 | 0.89 |
| germany | multiple_or_ambiguous | 0 | 0 | 0.01 |

## Occupation Buckets

| granular_bucket | bucket_family | entity_count | entity_pct |
| --- | --- | --- | --- |
| Writing / Literature | Literary and textual production | 14083 | 97.96 |
| Education / Scholarship / Humanities | Intellectual systems | 2476 | 17.22 |
| Religion / Theology | Intellectual systems | 2439 | 16.96 |
| Translation / Philology / Languages | Literary and textual production | 1355 | 9.42 |
| Law / Administration | Institutional power | 1094 | 7.61 |
| Politics / Statecraft / Diplomacy | Institutional power | 1040 | 7.23 |
| Print / Publishing / Journalism | Literary and textual production | 791 | 5.5 |
| Music / Performance / Theatre | Arts and performance | 665 | 4.63 |

Slice-level occupation bucket counts are written to
`data/processed/global_writers/core_findings_occupation_buckets_by_slice.csv`.

## Data Friction

| metric | entity_count | denominator | pct | notes |
| --- | --- | --- | --- | --- |
| missing_country_of_citizenship | 5302 | 14377 | 36.88 | No Wikidata P27 value in the enrichment export. |
| no_place_context | 3883 | 14377 | 27.01 | No birth/death/residence/work-location context rows recovered. |
| no_wikipedia_article_in_tracked_editions | 3818 | 14377 | 26.56 | No Wikipedia sitelink in the tracked language-edition matrix. |
| unresolved_name_label | 490 | 14377 | 3.41 | Name still appears as a raw Wikidata QID. |
| no_mapped_affiliation_candidate | 2248 | 14377 | 15.64 | No mapped candidate in the formula-backed affiliation evidence table. |
| top_affiliation_single_field_or_less | 4936 | 14377 | 34.33 | Top affiliation candidate is supported by zero or one evidence field. |

## Label And Description Coverage

| language_code | wiki_edition | wikidata_label_pct | wikidata_description_pct | wikipedia_article_pct |
| --- | --- | --- | --- | --- |
| en | enwiki | 96.28 | 74.59 | 36.9 |
| de | dewiki | 76.32 | 38.71 | 27.68 |
| fr | frwiki | 73.37 | 40.31 | 24.21 |
| ru | ruwiki | 22.84 | 11.82 | 16.94 |
| it | itwiki | 56.17 | 36.68 | 14.25 |
| es | eswiki | 79.08 | 30.51 | 12.76 |
| sv | svwiki | 41.09 | 18.11 | 12.7 |
| pl | plwiki | 17.33 | 9.37 | 8.4 |
| pt | ptwiki | 38.23 | 18.06 | 6.77 |
| nl | nlwiki | 82.12 | 67.75 | 6.48 |
| uk | ukwiki | 6.78 | 14.69 | 6.04 |
| da | dawiki | 35.21 | 13.83 | 3.95 |

## Computational Visual Layer

The Phase 2 matrix/network layer is generated by
`scripts/visuals/01_build_computational_visual_layer.py`.

Static figures:

- [`context_evidence_matrix.svg`](../figures/context_evidence_matrix.svg)
- [`context_evidence_punchcard.svg`](../figures/context_evidence_punchcard.svg)
- [`language_representation_heatmap.svg`](../figures/language_representation_heatmap.svg)
- [`occupation_bucket_matrix.svg`](../figures/occupation_bucket_matrix.svg)

Processed visual data:

- [`visual_matrix_evidence_construction.csv`](../data/processed/global_writers/visual_matrix_evidence_construction.csv)
- [`visual_matrix_language_representation.csv`](../data/processed/global_writers/visual_matrix_language_representation.csv)
- [`visual_matrix_occupation_buckets.csv`](../data/processed/global_writers/visual_matrix_occupation_buckets.csv)
- [`visual_matrix_data_friction.csv`](../data/processed/global_writers/visual_matrix_data_friction.csv)
- [`visual_network_nodes.csv`](../data/processed/global_writers/visual_network_nodes.csv)
- [`visual_network_edges.csv`](../data/processed/global_writers/visual_network_edges.csv)

The Phase 2.5 insight layer is generated by
`scripts/analysis/09_build_insight_mining_packet.py` and documented in
[`insight_mining_packet.md`](insight_mining_packet.md).

## Current Argument Surface

The strongest current argument is methodological: the project can show how
different evidence choices alter the Enlightenment map. A single field such as
country of citizenship undercounts or distorts imperial, dynastic, colonial,
and missing-data contexts. The reproducible context slices let us compare
France, Germany, British, and China/Qing on the same footing while keeping each
supporting field visible.

## Purposeful Next Work

1. Review the political-entity crosswalk rows used in the context slices.
2. Review the occupation-bucket crosswalk rows marked for manual review.
3. Use the Phase 2 and Phase 2.5 figures to assemble the conference segment.
4. Do not add external sources until a specific conference claim cannot be
   answered from the current Wikidata/Wikipedia evidence.
