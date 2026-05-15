# Computational Visual Plan

This phase builds Hunter's conference segment around computational evidence
construction. The visual layer uses only the active `global_writers` Wikidata
and Wikipedia spine. BnF and other external authority systems remain out of
scope.

## Figure Order

1. Evidence construction matrix:
   shows how France, Germany, British, and China/Qing are assembled differently
   depending on the evidence field.
2. Citizenship and place punchcard:
   isolates the citizenship/place contrast and makes the British imperial
   case legible.
3. Wikipedia language representation heatmap:
   shows which language editions over- or under-represent each context slice
   relative to the global cohort baseline.
4. Occupation bucket matrix:
   shifts from nationality evidence to intellectual profile, including
   Religion / Theology as a headline bucket.

## Suggested Presenter Language

The through-line is not "Wikidata is wrong." The stronger claim is that
historical identity is constructed differently depending on which computational
evidence field we choose. Country of citizenship is clean and useful for some
cases, but it is historically weak around empires, dynasties, colonies, and
missing data. Place context, language evidence, and Wikipedia representation
change the map in visible ways.

For the British slice, the punchcard lets the audience see why citizenship is
not enough: a large number of entities are recovered only through place
context. For China/Qing, the current data behaves differently: the slice is
mostly recovered through citizenship, which makes it a useful contrast rather
than just another problem case.

The language-edition heatmap should be framed as representation, not ground
truth. It asks where a person has a Wikipedia article in the tracked European
language editions and compares that slice rate to the global baseline.

The occupation matrix is the bridge from evidence construction into humanities
interpretation. The corpus is not just "writers"; religion, education,
philosophy, science, politics, law, and print culture give us a way to show
networks of intellectual labor.

## Caveats

- All figures use the `global_writers` cohort: Wikidata humans born 1675-1775
  with occupation `writer` or a subclass of writer.
- Context slices are not mutually exclusive. One entity can appear in more
  than one context slice.
- Counts are entity counts, not occupation-label row counts.
- Wikipedia language representation means article presence in tracked language
  editions, not cultural ownership or scholarly importance.
- Low-evidence affiliation is formula-backed: it means the top affiliation
  candidate has support from zero or one mapped evidence field.
- The occupation buckets depend on the local crosswalk and should remain
  reviewable.

## Data Sources

- Core packet: `data/processed/global_writers/core_findings_*.csv`
- Context membership: `data/processed/global_writers/context_slice_membership.csv`
- Affiliation evidence: `data/processed/global_writers/cultural_affiliation_evidence_matrix.csv`
- Occupation buckets: `data/processed/global_writers/occupation_bucket_entities_long.csv`

## Generated Outputs

Processed visual data:

- `data/processed/global_writers/visual_matrix_evidence_construction.csv`
- `data/processed/global_writers/visual_matrix_language_representation.csv`
- `data/processed/global_writers/visual_matrix_occupation_buckets.csv`
- `data/processed/global_writers/visual_matrix_data_friction.csv`
- `data/processed/global_writers/visual_network_nodes.csv`
- `data/processed/global_writers/visual_network_edges.csv`

Static figures:

- `figures/context_evidence_matrix.svg`
- `figures/context_evidence_punchcard.svg`
- `figures/language_representation_heatmap.svg`
- `figures/occupation_bucket_matrix.svg`
- `figures/gender_context_matrix.svg`
- `figures/occupation_overrepresentation_index.svg`
- `figures/decade_trends.svg`
- `figures/multi_context_entities_matrix.svg`
- `figures/data_friction_by_context.svg`

Regenerate with:

```powershell
python scripts/analysis/08_build_core_findings_packet.py
python scripts/visuals/01_build_computational_visual_layer.py
python scripts/analysis/09_build_insight_mining_packet.py
python scripts/visuals/02_build_insight_figures.py
```
