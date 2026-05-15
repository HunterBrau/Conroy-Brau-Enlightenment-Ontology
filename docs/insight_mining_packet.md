# Insight Mining Packet

Generated from the current `global_writers` ontology. This packet does not
pull new data. It turns the existing Wikidata/Wikipedia evidence spine into a
conference-facing set of claim candidates for a 20-25 minute computational
humanities segment.

## TL;DR

- The current ontology yields six strong claim candidates before any new data
  pull is needed.
- The recommended metadata gate is `defer_wikipedia_metadata_pull`.
- The strongest structure is: evidence construction, citizenship friction,
  gender representation, occupation/intellectual profile, language-edition
  visibility, decade trends, and multi-context examples.
- Context slices are non-exclusive. Overlap is a methodological feature, not a
  defect.

## Ranked Claim Candidates

| rank | claim_title | signal_strength | claim_text | figure |
| --- | --- | --- | --- | --- |
| 1 | Historical context changes with the evidence field. | strong | Germany has 2,343 place-only entities, France has 1,258, British has 645, while China/Qing has only 20. | context_evidence_punchcard.svg |
| 2 | Country of citizenship cannot carry the argument by itself. | strong | 5,302 entities (36.88%) lack citizenship; 4,936 (34.33%) have top affiliation support from one field or less. | data_friction_by_context.svg |
| 3 | Gender representation is not flat across context slices. | strong | Female share is 39.62% in China/Qing, 7.01% in Germany, against 12.12% globally. | gender_context_matrix.svg |
| 4 | Writerhood contains different intellectual profiles by context. | strong | China/Qing: Visual Arts / Architecture / Design (4.23x); France: Print / Publishing / Journalism (1.81x); France: Politics / Statecraft / Diplomacy (1.69x); Germany: Education / Scholarship / Humanities (1.65x); France: Philosophy (1.51x) | occupation_overrepresentation_index.svg |
| 5 | Wikipedia language editions reshape visibility. | strong | Top overrepresentation examples are France: frwiki (2.46x); British: enwiki (2.18x); Germany: dewiki (2.14x); France: nlwiki (2.05x). China/Qing's highest tracked edition is ukwiki at 1.01x global baseline. | language_representation_heatmap.svg |
| 6 | Birth-decade trends add a time axis to the ontology. | strong | The corpus starts with 313 entities in the 1670s; female share peaks at 14.32% in the 1760s. | decade_trends.svg |
| 7 | Multi-context entities expose crossings between national and imperial frames. | supporting | 766 entities appear in more than one reviewed context slice. | multi_context_entities_matrix.svg |
| 8 | The next data pull should be gated, not automatic. | gate | The current ontology yields at least six strong claims before article-depth metadata is needed. |  |

## Recommended 20-25 Minute Order

1. Start with the corpus and evidence construction matrix.
2. Use the citizenship/place punchcard to show why one field is not enough.
3. Move to the gender context matrix as the first representation result.
4. Show occupation overrepresentation to turn "writer" into intellectual
   profile.
5. Show language-edition representation as cross-language visibility.
6. Add birth-decade trends to give the ontology motion over time.
7. Use multi-context example entities to humanize the matrices.
8. Close with the metadata gate: pull more data only if article depth becomes
   necessary.

## Gender By Context

| context_label | slice_entities | slice_pct | global_pct | representation_index_vs_global |
| --- | --- | --- | --- | --- |
| France | 372 | 12.73 | 12.12 | 1.05 |
| Germany | 260 | 7.01 | 12.12 | 0.58 |
| British | 418 | 19.91 | 12.12 | 1.64 |
| China/Qing | 168 | 39.62 | 12.12 | 3.27 |

Suggested language: "Gender is not evenly distributed across the context
slices, and that difference is itself a computational finding. It asks us
whether the ontology, the source tradition, or the historical record is shaping
visibility."

## Occupation Overrepresentation

| context_label | granular_bucket | slice_entities | slice_pct | global_pct | index_vs_global |
| --- | --- | --- | --- | --- | --- |
| China/Qing | Visual Arts / Architecture / Design | 66 | 15.57 | 3.68 | 4.23 |
| France | Print / Publishing / Journalism | 291 | 9.96 | 5.5 | 1.81 |
| France | Politics / Statecraft / Diplomacy | 357 | 12.22 | 7.23 | 1.69 |
| Germany | Education / Scholarship / Humanities | 1055 | 28.43 | 17.22 | 1.65 |
| France | Philosophy | 138 | 4.72 | 3.13 | 1.51 |
| Germany | Religion / Theology | 938 | 25.28 | 16.96 | 1.49 |
| Germany | Law / Administration | 417 | 11.24 | 7.61 | 1.48 |
| France | Visual Arts / Architecture / Design | 156 | 5.34 | 3.68 | 1.45 |
| Germany | Philosophy | 168 | 4.53 | 3.13 | 1.45 |
| France | Law / Administration | 306 | 10.47 | 7.61 | 1.38 |
| France | Translation / Philology / Languages | 378 | 12.94 | 9.42 | 1.37 |
| France | Science / Natural History | 156 | 5.34 | 3.91 | 1.37 |

Suggested language: "The project does not just count writers. It decomposes the
writer cohort into intellectual labor: religion, education, print, politics,
law, science, philosophy, and the arts."

## Example Entities

| example_group | name | birth_year | gender_category | context_labels | wikipedia_sitelink_count |
| --- | --- | --- | --- | --- | --- |
| multi_context_high_visibility | Joshua Reynolds | 1723 | male | France \| Germany \| British | 61 |
| multi_context_high_visibility | Élisabeth Vigée Le Brun | 1755 | female | France \| Germany \| British | 61 |
| multi_context_high_visibility | Johann Heinrich Füssli | 1741 | male | France \| Germany \| British | 45 |
| multi_context_high_visibility | Lorenzo Da Ponte | 1749 | male | France \| Germany \| British | 44 |
| multi_context_high_visibility | Johann Lorenz Natter | 1705 | male | France \| Germany \| British | 7 |
| multi_context_high_visibility | James Forbes | 1749 | male | France \| Germany \| British | 7 |
| multi_context_high_visibility | Jean-Bernard Le Blanc | 1707 | male | France \| Germany \| British | 3 |
| multi_context_high_visibility | Anton Bemetzrieder | 1739 | male | France \| Germany \| British | 3 |
| british_place_only | Élisabeth Vigée Le Brun | 1755 | female | France \| Germany \| British | 61 |
| british_place_only | Lorenzo Da Ponte | 1749 | male | France \| Germany \| British | 44 |
| british_place_only | Johann Lorenz Natter | 1705 | male | France \| Germany \| British | 7 |
| british_place_only | Jean-Bernard Le Blanc | 1707 | male | France \| Germany \| British | 3 |
| british_place_only | Anton Bemetzrieder | 1739 | male | France \| Germany \| British | 3 |
| british_place_only | Hebelius Potter | 1768 | male | France \| Germany \| British | 2 |
| british_place_only | Tommaso Medin | 1725 | male | France \| Germany \| British | 1 |
| british_place_only | Jean-Godefroy Würtz | 1768 | male | France \| Germany \| British | 0 |
| germany_place_only | Joshua Reynolds | 1723 | male | France \| Germany \| British | 61 |
| germany_place_only | Élisabeth Vigée Le Brun | 1755 | female | France \| Germany \| British | 61 |
| germany_place_only | Johann Heinrich Füssli | 1741 | male | France \| Germany \| British | 45 |
| germany_place_only | Lorenzo Da Ponte | 1749 | male | France \| Germany \| British | 44 |
| germany_place_only | James Forbes | 1749 | male | France \| Germany \| British | 7 |
| germany_place_only | Jean-Bernard Le Blanc | 1707 | male | France \| Germany \| British | 3 |
| germany_place_only | Anton Bemetzrieder | 1739 | male | France \| Germany \| British | 3 |
| germany_place_only | Susanna Whatman | 1753 | female | France \| Germany \| British | 2 |

## Metadata Gate

| metric | value | threshold | recommendation |
| --- | --- | --- | --- |
| strong_claim_candidates | 6 | 6 | defer_wikipedia_metadata_pull |
| article_presence_limitation | known | claim requires article depth | collect_later_if_language_claim_needs_depth |
| future_metadata_fields | page_id \| article_length \| lead_extract \| categories \| revision_count | existing sitelinks only | scope_to_existing_wikipedia_sitelinks |
| phase_2_5_decision | defer_wikipedia_metadata_pull | Mine Then Gate | defer_wikipedia_metadata_pull |

Current recommendation: do not pull Wikipedia article metadata yet. The next
pull should happen only if article presence is too coarse for the language
representation claim. If needed, the future pull should be limited to existing
sitelinks and should collect page ID, article length, lead extract, categories,
and revision count.

## Caveats

- Context slices are not mutually exclusive.
- Wikipedia article presence is visibility evidence, not article depth or
  cultural value.
- Gender, occupation, and affiliation categories are Wikidata-derived and
  should be described as structured-data evidence.
- Decade trends use birth year, not publication year or period of activity.
- Occupation buckets are reviewable crosswalk categories.
