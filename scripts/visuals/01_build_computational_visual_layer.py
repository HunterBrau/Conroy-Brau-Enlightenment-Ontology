"""
Build slide-ready computational matrix and network data for the global cohort.

This visual layer stays deliberately narrow: it consumes the current
`global_writers` processed packet, writes matrix/network CSVs, and renders
static SVG figures. It does not broaden the corpus or add external authority
systems.
"""

from pathlib import Path
import sys

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import DEFAULT_COHORT_ID, cohort_paths  # noqa: E402
from common import bool_value, percentage  # noqa: E402


CONTEXT_ORDER = ["france", "germany", "british", "china"]
CONTEXT_LABELS = {
    "france": "France",
    "germany": "Germany",
    "british": "British",
    "china": "China/Qing",
}

EVIDENCE_FIELDS = [
    {
        "field_id": "citizenship",
        "field_label": "Citizenship",
        "field_group": "entity evidence",
    },
    {
        "field_id": "place_context",
        "field_label": "Place context",
        "field_group": "entity evidence",
    },
    {
        "field_id": "birth_place_context",
        "field_label": "Birth-place context",
        "field_group": "entity evidence",
    },
    {
        "field_id": "place_only",
        "field_label": "Place-only",
        "field_group": "derived evidence",
    },
    {
        "field_id": "language_evidence",
        "field_label": "Language evidence",
        "field_group": "entity evidence",
    },
    {
        "field_id": "wikipedia_representation",
        "field_label": "Wikipedia representation",
        "field_group": "representation evidence",
    },
    {
        "field_id": "missing_citizenship",
        "field_label": "Missing citizenship",
        "field_group": "data friction",
    },
    {
        "field_id": "low_evidence_affiliation",
        "field_label": "Low-evidence affiliation",
        "field_group": "data friction",
    },
]

PUNCHCARD_FIELDS = [
    "citizenship",
    "place_context",
    "birth_place_context",
    "place_only",
]

HEADLINE_OCCUPATION_BUCKETS = [
    "Writing / Literature",
    "Religion / Theology",
    "Education / Scholarship / Humanities",
    "Philosophy",
    "Science / Natural History",
    "Politics / Statecraft / Diplomacy",
    "Law / Administration",
    "Print / Publishing / Journalism",
]

FRICTION_METRICS = [
    ("missing_country_of_citizenship", "Missing citizenship"),
    ("no_place_context", "No place context"),
    ("no_wikipedia_article_in_tracked_editions", "No tracked Wikipedia article"),
    ("unresolved_name_label", "Unresolved name label"),
    ("no_mapped_affiliation_candidate", "No mapped affiliation candidate"),
    ("top_affiliation_single_field_or_less", "Top affiliation <= 1 field"),
]

LANGUAGE_LABELS = {
    "enwiki": "English",
    "dewiki": "German",
    "frwiki": "French",
    "ruwiki": "Russian",
    "itwiki": "Italian",
    "eswiki": "Spanish",
    "svwiki": "Swedish",
    "plwiki": "Polish",
    "ptwiki": "Portuguese",
    "nlwiki": "Dutch",
    "ukwiki": "Ukrainian",
    "dawiki": "Danish",
}

OUTPUT_FILES = {
    "evidence": "visual_matrix_evidence_construction.csv",
    "language": "visual_matrix_language_representation.csv",
    "occupation": "visual_matrix_occupation_buckets.csv",
    "friction": "visual_matrix_data_friction.csv",
    "nodes": "visual_network_nodes.csv",
    "edges": "visual_network_edges.csv",
}


def read_tables(processed_dir: Path) -> dict[str, pd.DataFrame]:
    filenames = {
        "context": "core_findings_context_slices.csv",
        "friction": "core_findings_data_friction.csv",
        "language": "core_findings_language_by_slice.csv",
        "occupation": "core_findings_occupation_buckets_by_slice.csv",
        "membership": "context_slice_membership.csv",
        "affiliation": "cultural_affiliation_evidence_matrix.csv",
    }
    tables = {}
    for key, filename in filenames.items():
        path = processed_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Required input is missing: {path}")
        tables[key] = pd.read_csv(path)
    return tables


def context_label(slice_id: str) -> str:
    return CONTEXT_LABELS.get(slice_id, slice_id)


def slug(value: str) -> str:
    return (
        value.lower()
        .replace(" / ", "_")
        .replace("/", "_")
        .replace(" ", "_")
        .replace("<=", "lte")
        .replace("-", "_")
    )


def field_lookup() -> dict[str, dict[str, str]]:
    return {field["field_id"]: field for field in EVIDENCE_FIELDS}


def ordered_context_frame(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    output["context_order"] = output["slice_id"].map({value: index for index, value in enumerate(CONTEXT_ORDER)})
    return output.sort_values("context_order")


def language_evidence_by_person(affiliation: pd.DataFrame) -> pd.Series:
    language_columns = [
        "native_language_available",
        "spoken_written_language_available",
        "writing_language_available",
    ]
    available = affiliation[["wikidata_id", *language_columns]].copy()
    for column in language_columns:
        available[column] = available[column].apply(bool_value)
    available["has_language_evidence"] = available[language_columns].any(axis=1)
    return available.groupby("wikidata_id")["has_language_evidence"].any()


def slice_language_counts(tables: dict[str, pd.DataFrame]) -> dict[str, int]:
    has_language = language_evidence_by_person(tables["affiliation"])
    membership = tables["membership"]
    counts = {}
    for slice_id, group in membership.groupby("slice_id", sort=False):
        ids = group["wikidata_id"].drop_duplicates()
        counts[slice_id] = int(has_language.reindex(ids, fill_value=False).sum())
    return counts


def friction_lookup(friction: pd.DataFrame) -> dict[tuple[str, str], pd.Series]:
    return {
        (row.scope_id, row.metric): row
        for row in friction.itertuples(index=False)
    }


def friction_metric_value(lookup: dict[tuple[str, str], pd.Series], slice_id: str, metric: str) -> tuple[int, int, float]:
    row = lookup.get((slice_id, metric))
    if row is None:
        return 0, 0, 0.0
    return int(row.entity_count), int(row.denominator), float(row.pct)


def build_evidence_matrix(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    context = tables["context"].set_index("slice_id")
    friction = friction_lookup(tables["friction"])
    language_counts = slice_language_counts(tables)
    fields = field_lookup()
    rows = []

    for slice_id in CONTEXT_ORDER:
        if slice_id not in context.index:
            continue
        source = context.loc[slice_id]
        denominator = int(source["entities_with_any_slice_evidence"])

        values = {
            "citizenship": int(source["entities_with_citizenship_evidence"]),
            "place_context": int(source["entities_with_place_context_evidence"]),
            "birth_place_context": int(source["entities_with_birth_place_context_evidence"]),
            "place_only": int(source["entities_with_place_context_only"]),
            "language_evidence": language_counts.get(slice_id, 0),
        }

        no_wiki_count, _, no_wiki_pct = friction_metric_value(
            friction,
            slice_id,
            "no_wikipedia_article_in_tracked_editions",
        )
        values["wikipedia_representation"] = max(denominator - no_wiki_count, 0)

        missing_count, _, _ = friction_metric_value(
            friction,
            slice_id,
            "missing_country_of_citizenship",
        )
        values["missing_citizenship"] = missing_count

        low_evidence_count, _, _ = friction_metric_value(
            friction,
            slice_id,
            "top_affiliation_single_field_or_less",
        )
        values["low_evidence_affiliation"] = low_evidence_count

        for field_order, field in enumerate(EVIDENCE_FIELDS):
            field_id = field["field_id"]
            value_count = values[field_id]
            value_pct = percentage(value_count, denominator)
            if field_id == "wikipedia_representation":
                value_pct = round(100.0 - no_wiki_pct, 2)
            rows.append(
                {
                    "slice_id": slice_id,
                    "context_label": context_label(slice_id),
                    "context_total_entities": denominator,
                    "field_id": field_id,
                    "field_label": fields[field_id]["field_label"],
                    "field_group": fields[field_id]["field_group"],
                    "field_order": field_order,
                    "value_count": value_count,
                    "denominator": denominator,
                    "value_pct": value_pct,
                    "notes": "Share of context slice entities with this evidence/friction condition.",
                }
            )

    return ordered_context_frame(pd.DataFrame(rows)).sort_values(["context_order", "field_order"])


def build_language_matrix(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    language = tables["language"].copy()
    language = language[language["slice_id"].isin(CONTEXT_ORDER)].copy()
    global_order = (
        language[["language_edition", "global_representation_pct"]]
        .drop_duplicates()
        .sort_values("global_representation_pct", ascending=False)
        ["language_edition"]
        .tolist()
    )
    language_order = {language_id: index for index, language_id in enumerate(global_order)}
    language["context_label"] = language["slice_id"].map(context_label)
    language["language_label"] = language["language_edition"].map(LANGUAGE_LABELS).fillna(language["language_edition"])
    language["language_order"] = language["language_edition"].map(language_order)
    language = ordered_context_frame(language)
    return language.sort_values(["context_order", "language_order"])


def build_occupation_matrix(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    occupation = tables["occupation"].copy()
    occupation = occupation[
        occupation["slice_id"].isin(CONTEXT_ORDER)
        & occupation["granular_bucket"].isin(HEADLINE_OCCUPATION_BUCKETS)
    ].copy()
    totals = (
        tables["context"]
        .loc[tables["context"]["slice_id"].isin(CONTEXT_ORDER), ["slice_id", "entities_with_any_slice_evidence"]]
        .set_index("slice_id")["entities_with_any_slice_evidence"]
        .to_dict()
    )
    bucket_family = (
        occupation[["granular_bucket", "bucket_family"]]
        .drop_duplicates()
        .set_index("granular_bucket")["bucket_family"]
        .to_dict()
    )
    rows = []
    for slice_id in CONTEXT_ORDER:
        for bucket_order, bucket in enumerate(HEADLINE_OCCUPATION_BUCKETS):
            match = occupation[
                (occupation["slice_id"] == slice_id)
                & (occupation["granular_bucket"] == bucket)
            ]
            total = int(totals.get(slice_id, 0))
            if match.empty:
                entity_count = 0
                pct = 0.0
            else:
                entity_count = int(match["slice_entities"].iloc[0])
                pct = float(match["slice_pct"].iloc[0])
            rows.append(
                {
                    "slice_id": slice_id,
                    "context_order": CONTEXT_ORDER.index(slice_id),
                    "context_label": context_label(slice_id),
                    "bucket": bucket,
                    "bucket_order": bucket_order,
                    "bucket_family": bucket_family.get(bucket, ""),
                    "slice_entities": entity_count,
                    "slice_total_entities": total,
                    "slice_pct": pct,
                }
            )
    return pd.DataFrame(rows)


def build_data_friction_matrix(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    friction = tables["friction"].copy()
    metric_label = dict(FRICTION_METRICS)
    metric_order = {metric: index for index, (metric, _) in enumerate(FRICTION_METRICS)}
    friction = friction[
        friction["scope_id"].isin(CONTEXT_ORDER)
        & friction["metric"].isin(metric_label)
    ].copy()
    friction["slice_id"] = friction["scope_id"]
    friction["context_label"] = friction["slice_id"].map(context_label)
    friction["metric_label"] = friction["metric"].map(metric_label)
    friction["metric_order"] = friction["metric"].map(metric_order)
    friction = ordered_context_frame(friction)
    return friction.sort_values(["context_order", "metric_order"])


def add_node(nodes: dict[str, dict], node_id: str, label: str, node_type: str, group: str, weight: float, notes: str) -> None:
    nodes[node_id] = {
        "node_id": node_id,
        "label": label,
        "node_type": node_type,
        "group": group,
        "weight": round(float(weight), 4),
        "notes": notes,
    }


def build_network_tables(
    evidence: pd.DataFrame,
    language: pd.DataFrame,
    occupation: pd.DataFrame,
    friction: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    nodes: dict[str, dict] = {}
    edges = []

    for row in evidence.drop_duplicates("slice_id").itertuples(index=False):
        add_node(
            nodes,
            f"slice:{row.slice_id}",
            row.context_label,
            "context_slice",
            "context",
            row.context_total_entities,
            "Reviewed context slice within the global writer cohort.",
        )

    for field in EVIDENCE_FIELDS:
        if field["field_group"] == "data friction":
            continue
        add_node(
            nodes,
            f"evidence:{field['field_id']}",
            field["field_label"],
            "evidence_type",
            field["field_group"],
            0,
            "Evidence type used to construct context-slice membership or interpretability.",
        )

    for _, label in FRICTION_METRICS:
        metric_id = slug(label)
        add_node(
            nodes,
            f"friction:{metric_id}",
            label,
            "data_friction_metric",
            "data friction",
            0,
            "Data-friction metric used to show limits of evidence construction.",
        )

    for row in language.drop_duplicates("language_edition").itertuples(index=False):
        add_node(
            nodes,
            f"language:{row.language_edition}",
            row.language_label,
            "language_edition",
            "Wikipedia",
            row.global_representation_pct,
            "Tracked Wikipedia language edition.",
        )

    for row in occupation.drop_duplicates("bucket").itertuples(index=False):
        add_node(
            nodes,
            f"occupation:{slug(row.bucket)}",
            row.bucket,
            "occupation_bucket",
            row.bucket_family,
            0,
            "Headline occupation bucket used in slide-ready matrix.",
        )

    for row in evidence.itertuples(index=False):
        if row.field_group == "data friction":
            continue
        edges.append(
            {
                "source": f"slice:{row.slice_id}",
                "target": f"evidence:{row.field_id}",
                "edge_type": "slice_to_evidence",
                "weight_count": int(row.value_count),
                "weight_pct": float(row.value_pct),
                "weight_index": pd.NA,
                "notes": "Share of slice entities with the evidence type.",
            }
        )

    for row in language.itertuples(index=False):
        edges.append(
            {
                "source": f"slice:{row.slice_id}",
                "target": f"language:{row.language_edition}",
                "edge_type": "slice_to_language",
                "weight_count": int(row.represented_entities),
                "weight_pct": float(row.slice_representation_pct),
                "weight_index": float(row.slice_to_global_representation_index),
                "notes": "Language-edition representation relative to the global baseline.",
            }
        )

    for row in occupation.itertuples(index=False):
        if int(row.slice_entities) == 0:
            continue
        edges.append(
            {
                "source": f"slice:{row.slice_id}",
                "target": f"occupation:{slug(row.bucket)}",
                "edge_type": "slice_to_occupation_bucket",
                "weight_count": int(row.slice_entities),
                "weight_pct": float(row.slice_pct),
                "weight_index": pd.NA,
                "notes": "Share of slice entities tagged with the occupation bucket.",
            }
        )

    friction_id_by_metric = {
        metric: slug(label)
        for metric, label in FRICTION_METRICS
    }
    for row in friction.itertuples(index=False):
        edges.append(
            {
                "source": f"slice:{row.slice_id}",
                "target": f"friction:{friction_id_by_metric[row.metric]}",
                "edge_type": "slice_to_data_friction",
                "weight_count": int(row.entity_count),
                "weight_pct": float(row.pct),
                "weight_index": pd.NA,
                "notes": row.notes,
            }
        )

    return pd.DataFrame(nodes.values()).sort_values(["node_type", "node_id"]), pd.DataFrame(edges)


def setup_plot_style() -> None:
    plt.rcParams.update(
        {
            "svg.fonttype": "none",
            "font.family": "DejaVu Sans",
            "font.size": 9,
            "axes.titlesize": 14,
            "axes.labelsize": 9,
            "axes.edgecolor": "#333333",
            "axes.linewidth": 0.6,
            "xtick.color": "#222222",
            "ytick.color": "#222222",
            "figure.facecolor": "white",
            "axes.facecolor": "white",
        }
    )


def pivot_matrix(
    df: pd.DataFrame,
    row_column: str,
    column_column: str,
    value_column: str,
    row_order: list[str],
    column_order: list[str],
) -> pd.DataFrame:
    matrix = (
        df.pivot_table(index=row_column, columns=column_column, values=value_column, aggfunc="first")
        .reindex(index=row_order, columns=column_order)
        .fillna(0)
    )
    return matrix


def annotate_heatmap(ax, matrix: pd.DataFrame, fmt: str = "{:.0f}", suffix: str = "%") -> None:
    values = matrix.to_numpy(dtype=float)
    threshold = np.nanmax(values) * 0.55 if values.size else 0
    for row_index, row_label in enumerate(matrix.index):
        for col_index, col_label in enumerate(matrix.columns):
            value = values[row_index, col_index]
            color = "white" if value >= threshold and threshold > 0 else "#1d1d1b"
            ax.text(
                col_index,
                row_index,
                f"{fmt.format(value)}{suffix}",
                ha="center",
                va="center",
                fontsize=8,
                color=color,
            )


def draw_heatmap(
    matrix: pd.DataFrame,
    output_path: Path,
    title: str,
    colorbar_label: str,
    *,
    cmap,
    norm=None,
    fmt: str = "{:.0f}",
    suffix: str = "%",
    figsize: tuple[float, float] = (12, 4.8),
) -> None:
    fig, ax = plt.subplots(figsize=figsize)
    image = ax.imshow(matrix.to_numpy(dtype=float), cmap=cmap, norm=norm, aspect="auto")
    ax.set_xticks(np.arange(len(matrix.columns)))
    ax.set_xticklabels(matrix.columns, rotation=35, ha="right", rotation_mode="anchor")
    ax.set_yticks(np.arange(len(matrix.index)))
    ax.set_yticklabels(matrix.index)
    ax.set_title(title, pad=14)
    ax.tick_params(axis="both", length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_xticks(np.arange(-0.5, len(matrix.columns), 1), minor=True)
    ax.set_yticks(np.arange(-0.5, len(matrix.index), 1), minor=True)
    ax.grid(which="minor", color="white", linestyle="-", linewidth=1.2)
    ax.tick_params(which="minor", bottom=False, left=False)
    annotate_heatmap(ax, matrix, fmt=fmt, suffix=suffix)
    colorbar = fig.colorbar(image, ax=ax, fraction=0.027, pad=0.025)
    colorbar.set_label(colorbar_label)
    colorbar.outline.set_visible(False)
    fig.tight_layout()
    fig.savefig(output_path, format="svg")
    plt.close(fig)


def draw_punchcard(evidence: pd.DataFrame, output_path: Path) -> None:
    plot_df = evidence[evidence["field_id"].isin(PUNCHCARD_FIELDS)].copy()
    field_labels = [field_lookup()[field_id]["field_label"] for field_id in PUNCHCARD_FIELDS]
    matrix = pivot_matrix(
        plot_df,
        "context_label",
        "field_label",
        "value_pct",
        [CONTEXT_LABELS[item] for item in CONTEXT_ORDER],
        field_labels,
    )
    count_matrix = pivot_matrix(
        plot_df,
        "context_label",
        "field_label",
        "value_count",
        [CONTEXT_LABELS[item] for item in CONTEXT_ORDER],
        field_labels,
    )

    cmap = LinearSegmentedColormap.from_list("punch_teal", ["#e9ece6", "#84b7a7", "#205c64"])
    fig, ax = plt.subplots(figsize=(9.5, 4.6))
    x_values = []
    y_values = []
    sizes = []
    colors = []
    for row_index, _ in enumerate(matrix.index):
        for col_index, _ in enumerate(matrix.columns):
            pct = float(matrix.iloc[row_index, col_index])
            x_values.append(col_index)
            y_values.append(row_index)
            sizes.append(max(pct, 2) * 14)
            colors.append(pct)
    scatter = ax.scatter(
        x_values,
        y_values,
        s=sizes,
        c=colors,
        cmap=cmap,
        vmin=0,
        vmax=100,
        edgecolors="#2f3f3f",
        linewidths=0.5,
    )
    for row_index, row_label in enumerate(matrix.index):
        for col_index, col_label in enumerate(matrix.columns):
            ax.text(
                col_index,
                row_index,
                f"{int(count_matrix.iloc[row_index, col_index]):,}",
                ha="center",
                va="center",
                fontsize=7,
                color="white" if matrix.iloc[row_index, col_index] >= 55 else "#1d1d1b",
            )
    ax.set_xticks(np.arange(len(matrix.columns)))
    ax.set_xticklabels(matrix.columns, rotation=25, ha="right")
    ax.set_yticks(np.arange(len(matrix.index)))
    ax.set_yticklabels(matrix.index)
    ax.set_xlim(-0.6, len(matrix.columns) - 0.4)
    ax.set_ylim(len(matrix.index) - 0.4, -0.6)
    ax.set_title("Citizenship and Place Evidence Punchcard", pad=14)
    ax.tick_params(axis="both", length=0)
    ax.grid(color="#d8d8d2", linewidth=0.7)
    for spine in ax.spines.values():
        spine.set_visible(False)
    colorbar = fig.colorbar(scatter, ax=ax, fraction=0.038, pad=0.03)
    colorbar.set_label("Slice share (%)")
    colorbar.outline.set_visible(False)
    fig.tight_layout()
    fig.savefig(output_path, format="svg")
    plt.close(fig)


def render_figures(
    evidence: pd.DataFrame,
    language: pd.DataFrame,
    occupation: pd.DataFrame,
    figures_dir: Path,
) -> None:
    figures_dir.mkdir(parents=True, exist_ok=True)
    setup_plot_style()
    pct_cmap = LinearSegmentedColormap.from_list("matrix_teal", ["#f5f4ed", "#8db8a8", "#225c63"])

    evidence_matrix = pivot_matrix(
        evidence,
        "context_label",
        "field_label",
        "value_pct",
        [CONTEXT_LABELS[item] for item in CONTEXT_ORDER],
        [field["field_label"] for field in EVIDENCE_FIELDS],
    )
    draw_heatmap(
        evidence_matrix,
        figures_dir / "context_evidence_matrix.svg",
        "Evidence Construction Matrix",
        "Slice share (%)",
        cmap=pct_cmap,
        fmt="{:.0f}",
        suffix="%",
        figsize=(12, 4.8),
    )

    draw_punchcard(evidence, figures_dir / "context_evidence_punchcard.svg")

    language_columns = (
        language[["language_label", "language_order"]]
        .drop_duplicates()
        .sort_values("language_order")
        ["language_label"]
        .tolist()
    )
    language_matrix = pivot_matrix(
        language,
        "context_label",
        "language_label",
        "slice_to_global_representation_index",
        [CONTEXT_LABELS[item] for item in CONTEXT_ORDER],
        language_columns,
    )
    index_max = max(2.5, float(language_matrix.to_numpy(dtype=float).max()))
    index_norm = TwoSlopeNorm(vmin=0, vcenter=1, vmax=index_max)
    index_cmap = LinearSegmentedColormap.from_list(
        "index_balance",
        ["#c98362", "#f4f1e9", "#2f6f81"],
    )
    draw_heatmap(
        language_matrix,
        figures_dir / "language_representation_heatmap.svg",
        "Wikipedia Language Representation Index",
        "Index vs global baseline",
        cmap=index_cmap,
        norm=index_norm,
        fmt="{:.2f}",
        suffix="",
        figsize=(12, 4.8),
    )

    occupation_matrix = pivot_matrix(
        occupation,
        "context_label",
        "bucket",
        "slice_pct",
        [CONTEXT_LABELS[item] for item in CONTEXT_ORDER],
        HEADLINE_OCCUPATION_BUCKETS,
    )
    draw_heatmap(
        occupation_matrix,
        figures_dir / "occupation_bucket_matrix.svg",
        "Occupation Bucket Matrix",
        "Slice share (%)",
        cmap=pct_cmap,
        fmt="{:.0f}",
        suffix="%",
        figsize=(13, 4.8),
    )


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, DEFAULT_COHORT_ID)
    if paths.cohort_id != "global_writers":
        raise SystemExit("The visual layer expects global_writers to be the active analytical cohort.")

    tables = read_tables(paths.processed_dir)
    evidence = build_evidence_matrix(tables)
    language = build_language_matrix(tables)
    occupation = build_occupation_matrix(tables)
    friction = build_data_friction_matrix(tables)
    nodes, edges = build_network_tables(evidence, language, occupation, friction)

    outputs = {
        "evidence": evidence,
        "language": language,
        "occupation": occupation,
        "friction": friction,
        "nodes": nodes,
        "edges": edges,
    }
    for key, frame in outputs.items():
        output_path = paths.processed_dir / OUTPUT_FILES[key]
        frame.to_csv(output_path, index=False)

    render_figures(evidence, language, occupation, project_root / "figures")

    print("Computational visual layer complete.")
    print(f"Cohort: {paths.cohort_id}")
    for key in OUTPUT_FILES:
        print(f"Output: {paths.processed_dir / OUTPUT_FILES[key]}")
    for figure in [
        "context_evidence_matrix.svg",
        "context_evidence_punchcard.svg",
        "language_representation_heatmap.svg",
        "occupation_bucket_matrix.svg",
    ]:
        print(f"Figure: {project_root / 'figures' / figure}")


if __name__ == "__main__":
    main()
