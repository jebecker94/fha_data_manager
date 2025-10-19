"""Network analytics for FHA sponsor-originator relationships."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Dict, Tuple

import networkx as nx
import polars as pl


logger = logging.getLogger(__name__)


EdgeFrame = pl.DataFrame
BipartiteSets = Tuple[set[str], set[str]]


def load_originator_sponsor_edges(
    data_path: str | Path,
    *,
    start_year: int | None = None,
    end_year: int | None = None,
    min_loans: int = 1,
) -> EdgeFrame:
    """Load aggregated sponsor-originator relationships from parquet data.

    Parameters
    ----------
    data_path:
        Location of the hive-structured parquet directory (e.g. ``data/database/single_family``).
    start_year, end_year:
        Optional temporal filters applied to the ``Year`` column to focus on a
        subset of the portfolio.
    min_loans:
        Minimum number of loans required for a sponsor-originator pair to be
        included in the results. This helps remove extremely small edges that
        could introduce noise into the network metrics.

    Returns
    -------
    pl.DataFrame
        Aggregated relationship table with one record per sponsor-originator
        pair, including activity counts and loan volume statistics.
    """

    lazy_df = pl.scan_parquet(str(data_path))

    if start_year is not None:
        lazy_df = lazy_df.filter(pl.col("Year") >= start_year)
    if end_year is not None:
        lazy_df = lazy_df.filter(pl.col("Year") <= end_year)

    logger.info("Loading originator-sponsor edges from %%s", data_path)

    edges = (
        lazy_df
        .filter(
            pl.col("Sponsor Name").is_not_null()
            & pl.col("Originating Mortgagee").is_not_null()
        )
        .with_columns([
            pl.when(pl.col("Originating Mortgagee Number").is_not_null())
            .then(pl.col("Originating Mortgagee Number").cast(pl.Utf8))
            .otherwise(pl.concat_str([
                pl.lit("originator:"),
                pl.col("Originating Mortgagee")
            ]))
            .alias("originator_key"),
            pl.when(pl.col("Sponsor Number").is_not_null())
            .then(pl.col("Sponsor Number").cast(pl.Utf8))
            .otherwise(pl.concat_str([
                pl.lit("sponsor:"),
                pl.col("Sponsor Name")
            ]))
            .alias("sponsor_key"),
        ])
        .group_by([
            "originator_key",
            "Originating Mortgagee",
            "Originating Mortgagee Number",
            "sponsor_key",
            "Sponsor Name",
            "Sponsor Number",
        ])
        .agg([
            pl.len().alias("loan_count"),
            pl.col("Mortgage Amount").sum().alias("total_volume"),
            pl.col("Mortgage Amount").mean().alias("avg_loan_amount"),
            pl.col("Mortgage Amount").median().alias("median_loan_amount"),
            pl.col("Year").min().alias("first_year"),
            pl.col("Year").max().alias("last_year"),
        ])
        .filter(pl.col("loan_count") >= min_loans)
        .sort(["loan_count", "total_volume"], descending=True)
        .collect()
    )

    logger.info("Identified %%s sponsor-originator edges", edges.height)
    return edges


def build_bipartite_graph(
    edges: EdgeFrame,
    *,
    weight_col: str = "loan_count",
) -> tuple[nx.Graph, BipartiteSets]:
    """Construct a bipartite NetworkX graph from aggregated relationship edges."""

    graph = nx.Graph()
    originators: set[str] = set()
    sponsors: set[str] = set()

    if edges.is_empty():
        return graph, (originators, sponsors)

    for row in edges.iter_rows(named=True):
        originator_key = row["originator_key"]
        sponsor_key = row["sponsor_key"]

        originators.add(originator_key)
        sponsors.add(sponsor_key)

        graph.add_node(
            originator_key,
            bipartite="originator",
            entity_id=row.get("Originating Mortgagee Number"),
            label=row.get("Originating Mortgagee"),
        )
        graph.add_node(
            sponsor_key,
            bipartite="sponsor",
            entity_id=row.get("Sponsor Number"),
            label=row.get("Sponsor Name"),
        )

        edge_attributes = {
            "loan_count": row.get("loan_count", 0),
            "total_volume": row.get("total_volume"),
            "avg_loan_amount": row.get("avg_loan_amount"),
            "median_loan_amount": row.get("median_loan_amount"),
            "first_year": row.get("first_year"),
            "last_year": row.get("last_year"),
        }
        edge_attributes["weight"] = row.get(weight_col, row.get("loan_count", 1))

        graph.add_edge(originator_key, sponsor_key, **edge_attributes)

    return graph, (originators, sponsors)


def _weighted_degree(graph: nx.Graph, node: str, weight_attr: str) -> float:
    """Compute the weighted degree for a node."""

    return sum(data.get(weight_attr, 0.0) for _, data in graph[node].items())


def compute_centrality_metrics(
    graph: nx.Graph,
    node_sets: BipartiteSets,
    *,
    weight_attr: str = "weight",
) -> Dict[str, pl.DataFrame]:
    """Compute centrality metrics for both sides of the bipartite graph."""

    originators, sponsors = node_sets

    if graph.number_of_nodes() == 0:
        return {
            "originator_centrality": pl.DataFrame([]),
            "sponsor_centrality": pl.DataFrame([]),
        }

    degree_cent_originators = nx.bipartite.degree_centrality(graph, originators)
    degree_cent_sponsors = nx.bipartite.degree_centrality(graph, sponsors)

    betweenness = nx.betweenness_centrality(graph, weight=weight_attr, normalized=True)

    originator_rows = []
    for node in originators:
        data = graph.nodes[node]
        originator_rows.append({
            "node": node,
            "originator_name": data.get("label"),
            "originator_id": data.get("entity_id"),
            "degree": graph.degree(node),
            "weighted_degree": _weighted_degree(graph, node, weight_attr),
            "degree_centrality": degree_cent_originators.get(node, 0.0),
            "betweenness": betweenness.get(node, 0.0),
        })

    sponsor_rows = []
    for node in sponsors:
        data = graph.nodes[node]
        sponsor_rows.append({
            "node": node,
            "sponsor_name": data.get("label"),
            "sponsor_id": data.get("entity_id"),
            "degree": graph.degree(node),
            "weighted_degree": _weighted_degree(graph, node, weight_attr),
            "degree_centrality": degree_cent_sponsors.get(node, 0.0),
            "betweenness": betweenness.get(node, 0.0),
        })

    originator_df = pl.DataFrame(originator_rows).sort(
        ["weighted_degree", "degree"], descending=True
    )
    sponsor_df = pl.DataFrame(sponsor_rows).sort(
        ["weighted_degree", "degree"], descending=True
    )

    return {
        "originator_centrality": originator_df,
        "sponsor_centrality": sponsor_df,
    }


def project_affiliation_graphs(
    graph: nx.Graph,
    node_sets: BipartiteSets,
    *,
    weight_attr: str = "weight",
) -> Dict[str, pl.DataFrame]:
    """Generate one-mode projections for originators and sponsors."""

    originators, sponsors = node_sets

    if graph.number_of_nodes() == 0:
        empty = pl.DataFrame([])
        return {"originator_projection": empty, "sponsor_projection": empty}

    originator_projection = nx.bipartite.weighted_projected_graph(
        graph, originators, weight_function=_projection_weight(weight_attr)
    )
    sponsor_projection = nx.bipartite.weighted_projected_graph(
        graph, sponsors, weight_function=_projection_weight(weight_attr)
    )

    return {
        "originator_projection": _graph_to_edge_frame(originator_projection, weight_attr),
        "sponsor_projection": _graph_to_edge_frame(sponsor_projection, weight_attr),
    }


def _projection_weight(weight_attr: str):
    """Create a projection weight function that respects the edge weight attribute."""

    def weight_fn(graph: nx.Graph, u: str, v: str, shared_neighbors: Iterable[str]) -> float:
        weight = 0.0
        for neighbor in shared_neighbors:
            weight += (
                graph[u][neighbor].get(weight_attr, 1.0)
                + graph[v][neighbor].get(weight_attr, 1.0)
            ) / 2
        return weight

    return weight_fn


def _graph_to_edge_frame(graph: nx.Graph, weight_attr: str) -> pl.DataFrame:
    """Convert a NetworkX graph to an edge list DataFrame."""

    rows: list[dict[str, Any]] = []
    for u, v, data in graph.edges(data=True):
        rows.append({
            "source": u,
            "target": v,
            weight_attr: data.get(weight_attr, data.get("weight", 1.0)),
        })

    if not rows:
        return pl.DataFrame([])

    return pl.DataFrame(rows).sort(weight_attr, descending=True)


def analyze_sponsor_originator_network(
    data_path: str | Path,
    *,
    start_year: int | None = None,
    end_year: int | None = None,
    min_loans: int = 1,
    weight_col: str = "loan_count",
) -> Dict[str, Any]:
    """High-level helper that orchestrates the network analytics workflow."""

    edges = load_originator_sponsor_edges(
        data_path,
        start_year=start_year,
        end_year=end_year,
        min_loans=min_loans,
    )

    graph, node_sets = build_bipartite_graph(edges, weight_col=weight_col)
    centrality = compute_centrality_metrics(graph, node_sets, weight_attr="weight")
    projections = project_affiliation_graphs(graph, node_sets, weight_attr="weight")

    summary = {
        "edge_count": edges.height,
        "originator_nodes": len(node_sets[0]),
        "sponsor_nodes": len(node_sets[1]),
    }

    return {
        "edges": edges,
        "graph": graph,
        "centrality": centrality,
        "projections": projections,
        "summary": summary,
    }
