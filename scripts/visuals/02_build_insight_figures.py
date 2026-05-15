"""
Render second-layer insight figures from the Phase 2.5 insight packet.

The figures are static SVGs intended for slides. They use only processed
`global_writers` insight tables and do not fetch or broaden data.
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


CONTEXT_ORDER = ["france", "germany", "british", "china"]
CONTEXT_LABELS = {
    "france": "France",
    "germany": "Germany",
    "british": "British",
    "china": "China/Qing",
}
GENDER_ORDER = ["female", "male", "unknown", "multiple_or_ambiguous"]
GENDER_LABELS = {
    "female": "Female",
    "male": "Male",
    "unknown": "Unknown",
    "multiple_or_ambiguous": "Multiple/ambiguous",
}
INSIGHT_OCCUPATION_BUCKETS = [
    "Visual Arts / Architecture / Design",
    "Print / Publishing / Journalism",
    "Education / Scholarship / Humanities",
    "Religion / Theology",
    "Politics / Statecraft / Diplomacy",
    "Law / Administration",
    "Philosophy",
    "Science / Natural History",
    "Translation / Philology / Languages",
]
FRICTION_METRICS = [
    "missing_country_of_citizenship",
    "no_place_context",
    "no_tracked_wikipedia_article",
    "low_evidence_affiliation",
]
FRICTION_LABELS = {
    "missing_country_of_citizenship": "Missing citizenship",
    "no_place_context": "No place context",
    "no_tracked_wikipedia_article": "No tracked article",
    "low_evidence_affiliation": "Low-evidence affiliation",
}


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


def context_label(slice_id: str) -> str:
    return CONTEXT_LABELS.get(slice_id, slice_id)


def context_label_order() -> list[str]:
    return [CONTEXT_LABELS[slice_id] for slice_id in CONTEXT_ORDER]


def read_insights(processed_dir: Path) -> dict[str, pd.DataFrame]:
    filenames = {
        "gender": "insight_gender_context.csv",
        "occupation": "insight_occupation_overrepresentation.csv",
        "decade": "insight_decade_trends.csv",
        "multi": "insight_multi_context_entities.csv",
        "friction": "insight_data_friction_by_context_gender_bucket.csv",
    }
    tables = {}
    for key, filename in filenames.items():
        path = processed_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Required insight input is missing: {path}")
        tables[key] = pd.read_csv(path)
    return tables


def pivot_matrix(
    df: pd.DataFrame,
    row_column: str,
    column_column: str,
    value_column: str,
    row_order: list[str],
    column_order: list[str],
) -> pd.DataFrame:
    return (
        df.pivot_table(index=row_column, columns=column_column, values=value_column, aggfunc="first")
        .reindex(index=row_order, columns=column_order)
        .fillna(0)
    )


def annotate_heatmap(ax, matrix: pd.DataFrame, fmt: str, suffix: str = "") -> None:
    values = matrix.to_numpy(dtype=float)
    if not values.size:
        return
    threshold = np.nanmax(values) * 0.55
    for row_index in range(matrix.shape[0]):
        for col_index in range(matrix.shape[1]):
            value = values[row_index, col_index]
            color = "white" if threshold > 0 and value >= threshold else "#1d1d1b"
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
    figsize: tuple[float, float] = (10, 4.6),
) -> None:
    fig, ax = plt.subplots(figsize=figsize)
    image = ax.imshow(matrix.to_numpy(dtype=float), cmap=cmap, norm=norm, aspect="auto")
    ax.set_xticks(np.arange(len(matrix.columns)))
    ax.set_xticklabels(matrix.columns, rotation=32, ha="right", rotation_mode="anchor")
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
    colorbar = fig.colorbar(image, ax=ax, fraction=0.03, pad=0.025)
    colorbar.set_label(colorbar_label)
    colorbar.outline.set_visible(False)
    fig.tight_layout()
    fig.savefig(output_path, format="svg")
    plt.close(fig)


def draw_gender_context_matrix(gender: pd.DataFrame, figures_dir: Path) -> None:
    plot_df = gender.copy()
    plot_df["gender_label"] = plot_df["gender_category"].map(GENDER_LABELS).fillna(plot_df["gender_category"])
    matrix = pivot_matrix(
        plot_df,
        "context_label",
        "gender_label",
        "slice_pct",
        context_label_order(),
        [GENDER_LABELS[item] for item in GENDER_ORDER],
    )
    cmap = LinearSegmentedColormap.from_list("gender_teal", ["#f6f3ea", "#8fb7a7", "#255f67"])
    draw_heatmap(
        matrix,
        figures_dir / "gender_context_matrix.svg",
        "Gender Representation by Context",
        "Slice share (%)",
        cmap=cmap,
        fmt="{:.0f}",
        suffix="%",
        figsize=(9.5, 4.4),
    )


def draw_occupation_index(occupation: pd.DataFrame, figures_dir: Path) -> None:
    plot_df = occupation[occupation["granular_bucket"].isin(INSIGHT_OCCUPATION_BUCKETS)].copy()
    matrix = pivot_matrix(
        plot_df,
        "context_label",
        "granular_bucket",
        "index_vs_global",
        context_label_order(),
        INSIGHT_OCCUPATION_BUCKETS,
    )
    vmax = max(4.5, float(matrix.to_numpy(dtype=float).max()))
    cmap = LinearSegmentedColormap.from_list("occupation_index", ["#c9825f", "#f4f1e9", "#2f7081"])
    norm = TwoSlopeNorm(vmin=0, vcenter=1, vmax=vmax)
    draw_heatmap(
        matrix,
        figures_dir / "occupation_overrepresentation_index.svg",
        "Occupation Overrepresentation Index",
        "Index vs global baseline",
        cmap=cmap,
        norm=norm,
        fmt="{:.2f}",
        suffix="x",
        figsize=(13, 4.9),
    )


def draw_decade_trends(decade: pd.DataFrame, figures_dir: Path) -> None:
    global_df = decade.loc[decade["scope_id"] == "global_writers"].sort_values("birth_decade").copy()
    fig, (ax_count, ax_pct) = plt.subplots(
        2,
        1,
        figsize=(10, 6),
        sharex=True,
        gridspec_kw={"height_ratios": [1, 1.25]},
    )
    bars = ax_count.bar(
        global_df["birth_decade"].astype(int).astype(str),
        global_df["entities"],
        color="#6f9d9b",
        edgecolor="#2e4c4f",
        linewidth=0.6,
    )
    ax_count.set_title("Birth-Decade Trends in the Global Writer Cohort", pad=12)
    ax_count.set_ylabel("Entities")
    ax_count.bar_label(bars, fontsize=7, padding=2)
    ax_count.spines[["top", "right"]].set_visible(False)
    ax_count.grid(axis="y", color="#deded8", linewidth=0.7)

    ax_pct.plot(
        global_df["birth_decade"].astype(int).astype(str),
        global_df["female_pct"],
        marker="o",
        color="#8a4b5d",
        linewidth=2,
        label="Female share",
    )
    ax_pct.plot(
        global_df["birth_decade"].astype(int).astype(str),
        global_df["missing_citizenship_pct"],
        marker="o",
        color="#9a7a3c",
        linewidth=2,
        label="Missing citizenship",
    )
    ax_pct.plot(
        global_df["birth_decade"].astype(int).astype(str),
        global_df["wikipedia_article_pct"],
        marker="o",
        color="#2f7081",
        linewidth=2,
        label="Any tracked Wikipedia article",
    )
    ax_pct.set_ylabel("Share (%)")
    ax_pct.set_xlabel("Birth decade")
    ax_pct.set_ylim(0, 100)
    ax_pct.spines[["top", "right"]].set_visible(False)
    ax_pct.grid(axis="y", color="#deded8", linewidth=0.7)
    ax_pct.legend(frameon=False, ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.22))
    fig.tight_layout()
    fig.savefig(figures_dir / "decade_trends.svg", format="svg")
    plt.close(fig)


def draw_multi_context_matrix(multi: pd.DataFrame, figures_dir: Path) -> None:
    matrix = pd.DataFrame(0, index=context_label_order(), columns=context_label_order(), dtype=int)
    for row in multi.itertuples(index=False):
        slice_ids = [token.strip() for token in str(row.context_slice_ids).split("|") if token.strip()]
        labels = [context_label(slice_id) for slice_id in slice_ids if slice_id in CONTEXT_ORDER]
        for label in labels:
            matrix.loc[label, label] += 1
        for row_label in labels:
            for col_label in labels:
                if row_label != col_label:
                    matrix.loc[row_label, col_label] += 1
    cmap = LinearSegmentedColormap.from_list("overlap_teal", ["#f6f3ea", "#92b9aa", "#245d65"])
    draw_heatmap(
        matrix,
        figures_dir / "multi_context_entities_matrix.svg",
        "Multi-Context Entity Overlap",
        "Entity count",
        cmap=cmap,
        fmt="{:.0f}",
        suffix="",
        figsize=(7.4, 5.2),
    )


def draw_data_friction_context(friction: pd.DataFrame, figures_dir: Path) -> None:
    plot_df = friction.loc[
        (friction["aggregation_level"] == "context")
        & (friction["metric"].isin(FRICTION_METRICS))
    ].copy()
    plot_df["metric_label"] = plot_df["metric"].map(FRICTION_LABELS)
    matrix = pivot_matrix(
        plot_df,
        "context_label",
        "metric_label",
        "pct",
        context_label_order(),
        [FRICTION_LABELS[item] for item in FRICTION_METRICS],
    )
    cmap = LinearSegmentedColormap.from_list("friction_rust", ["#f7f2e9", "#cc9b72", "#744b45"])
    draw_heatmap(
        matrix,
        figures_dir / "data_friction_by_context.svg",
        "Data Friction by Context",
        "Slice share (%)",
        cmap=cmap,
        fmt="{:.0f}",
        suffix="%",
        figsize=(10, 4.5),
    )


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, DEFAULT_COHORT_ID)
    if paths.cohort_id != "global_writers":
        raise SystemExit("Insight figures expect global_writers to be the active analytical cohort.")

    tables = read_insights(paths.processed_dir)
    figures_dir = project_root / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    setup_plot_style()

    draw_gender_context_matrix(tables["gender"], figures_dir)
    draw_occupation_index(tables["occupation"], figures_dir)
    draw_decade_trends(tables["decade"], figures_dir)
    draw_multi_context_matrix(tables["multi"], figures_dir)
    draw_data_friction_context(tables["friction"], figures_dir)

    print("Insight figures complete.")
    for filename in [
        "gender_context_matrix.svg",
        "occupation_overrepresentation_index.svg",
        "decade_trends.svg",
        "multi_context_entities_matrix.svg",
        "data_friction_by_context.svg",
    ]:
        print(f"Figure: {figures_dir / filename}")


if __name__ == "__main__":
    main()
