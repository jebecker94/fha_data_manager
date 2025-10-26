"""
Example 6: Network Analysis of FHA Sponsor-Originator Relationships

This script demonstrates how to perform network analysis on FHA data to understand
the relationships between sponsors and originators. It shows how to:

1. Load sponsor-originator relationship data
2. Build bipartite graphs
3. Compute centrality metrics
4. Generate one-mode projections
5. Analyze network structure and key players
"""

from fha_data_manager.analysis.network import (
    analyze_sponsor_originator_network,
    load_originator_sponsor_edges,
    build_bipartite_graph,
    compute_centrality_metrics,
    project_affiliation_graphs,
)
import polars as pl
from pathlib import Path


def analyze_network_structure() -> None:
    """Analyze the overall network structure of sponsor-originator relationships."""
    print("\n" + "=" * 80)
    print("NETWORK STRUCTURE ANALYSIS")
    print("=" * 80)
    
    # Load edges with minimum threshold to focus on meaningful relationships
    print("\nLoading sponsor-originator relationships...")
    edges = load_originator_sponsor_edges(
        "data/silver/single_family",
        min_loans=5,  # Only include relationships with 5+ loans
        start_year=2020,  # Focus on recent data
    )
    
    print(f"Found {edges.height} sponsor-originator relationships")
    print(f"Total loan volume: ${edges['total_volume'].sum():,.0f}")
    
    # Show top relationships by loan count
    print("\nTop 10 Sponsor-Originator Relationships by Loan Count:")
    top_relationships = edges.head(10)
    for row in top_relationships.iter_rows(named=True):
        print(f"  {row['Sponsor Name']} ↔ {row['Originating Mortgagee']}")
        print(f"    Loans: {row['loan_count']:,}, Volume: ${row['total_volume']:,.0f}")
    
    return edges


def analyze_centrality_metrics(edges: pl.DataFrame) -> None:
    """Analyze centrality metrics to identify key players in the network."""
    print("\n" + "=" * 80)
    print("CENTRALITY ANALYSIS")
    print("=" * 80)
    
    # Build bipartite graph
    print("\nBuilding bipartite graph...")
    graph, node_sets = build_bipartite_graph(edges, weight_col="loan_count")
    
    print(f"Graph has {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges")
    print(f"Originators: {len(node_sets[0])}, Sponsors: {len(node_sets[1])}")
    
    # Compute centrality metrics
    print("\nComputing centrality metrics...")
    centrality = compute_centrality_metrics(graph, node_sets, weight_attr="weight")
    
    # Show top originators by weighted degree (most connected by loan volume)
    print("\nTop 10 Originators by Weighted Degree (Loan Volume):")
    top_originators = centrality["originator_centrality"].head(10)
    for row in top_originators.iter_rows(named=True):
        print(f"  {row['originator_name']} (ID: {row['originator_id']})")
        print(f"    Connections: {row['degree']}, Weighted Degree: {row['weighted_degree']:,.0f}")
        print(f"    Degree Centrality: {row['degree_centrality']:.3f}, Betweenness: {row['betweenness']:.3f}")
    
    # Show top sponsors by weighted degree
    print("\nTop 10 Sponsors by Weighted Degree (Loan Volume):")
    top_sponsors = centrality["sponsor_centrality"].head(10)
    for row in top_sponsors.iter_rows(named=True):
        print(f"  {row['sponsor_name']} (ID: {row['sponsor_id']})")
        print(f"    Connections: {row['degree']}, Weighted Degree: {row['weighted_degree']:,.0f}")
        print(f"    Degree Centrality: {row['degree_centrality']:.3f}, Betweenness: {row['betweenness']:.3f}")
    
    return graph, node_sets, centrality


def analyze_projections(graph, node_sets) -> None:
    """Analyze one-mode projections to understand intra-group relationships."""
    print("\n" + "=" * 80)
    print("ONE-MODE PROJECTION ANALYSIS")
    print("=" * 80)
    
    # Generate projections
    print("\nGenerating one-mode projections...")
    projections = project_affiliation_graphs(graph, node_sets, weight_attr="weight")
    
    # Analyze originator projection (originators connected through shared sponsors)
    originator_proj = projections["originator_projection"]
    if originator_proj.height > 0:
        print(f"\nOriginator Projection: {originator_proj.height} edges")
        print("Top 5 Originator-Originator Relationships (by shared sponsor activity):")
        for row in originator_proj.head(5).iter_rows(named=True):
            print(f"  {row['source']} ↔ {row['target']}")
            print(f"    Shared Activity Weight: {row['weight']:.1f}")
    else:
        print("\nOriginator Projection: No edges found")
    
    # Analyze sponsor projection (sponsors connected through shared originators)
    sponsor_proj = projections["sponsor_projection"]
    if sponsor_proj.height > 0:
        print(f"\nSponsor Projection: {sponsor_proj.height} edges")
        print("Top 5 Sponsor-Sponsor Relationships (by shared originator activity):")
        for row in sponsor_proj.head(5).iter_rows(named=True):
            print(f"  {row['source']} ↔ {row['target']}")
            print(f"    Shared Activity Weight: {row['weight']:.1f}")
    else:
        print("\nSponsor Projection: No edges found")


def run_comprehensive_network_analysis() -> None:
    """Run a comprehensive network analysis using the high-level helper function."""
    print("\n" + "=" * 80)
    print("COMPREHENSIVE NETWORK ANALYSIS")
    print("=" * 80)
    
    print("\nRunning comprehensive analysis (this may take a moment)...")
    results = analyze_sponsor_originator_network(
        "data/silver/single_family",
        start_year=2020,
        min_loans=3,
        weight_col="loan_count"
    )
    
    # Display summary
    summary = results["summary"]
    print(f"\nNetwork Summary:")
    print(f"  Total edges: {summary['edge_count']:,}")
    print(f"  Originator nodes: {summary['originator_nodes']:,}")
    print(f"  Sponsor nodes: {summary['sponsor_nodes']:,}")
    
    # Show network density
    graph = results["graph"]
    if graph.number_of_nodes() > 0:
        density = graph.number_of_edges() / (graph.number_of_nodes() * (graph.number_of_nodes() - 1) / 2)
        print(f"  Network density: {density:.6f}")
    
    # Save results to output directory
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    print(f"\nSaving network analysis results to {output_dir}/...")
    
    # Save edge data
    results["edges"].write_csv(output_dir / "network_edges.csv")
    
    # Save centrality metrics
    results["centrality"]["originator_centrality"].write_csv(
        output_dir / "originator_centrality.csv"
    )
    results["centrality"]["sponsor_centrality"].write_csv(
        output_dir / "sponsor_centrality.csv"
    )
    
    # Save projections
    if results["projections"]["originator_projection"].height > 0:
        results["projections"]["originator_projection"].write_csv(
            output_dir / "originator_projection.csv"
        )
    if results["projections"]["sponsor_projection"].height > 0:
        results["projections"]["sponsor_projection"].write_csv(
            output_dir / "sponsor_projection.csv"
        )
    
    print("✓ Network analysis results saved!")


def main() -> None:
    """Run all network analysis examples."""
    print("=" * 80)
    print("FHA NETWORK ANALYSIS EXAMPLES")
    print("=" * 80)
    
    try:
        # Step 1: Analyze network structure
        edges = analyze_network_structure()
        
        # Step 2: Analyze centrality metrics
        graph, node_sets, centrality = analyze_centrality_metrics(edges)
        
        # Step 3: Analyze projections
        analyze_projections(graph, node_sets)
        
        # Step 4: Run comprehensive analysis
        run_comprehensive_network_analysis()
        
        print("\n" + "=" * 80)
        print("Network analysis complete!")
        print("=" * 80)
        print("\nCheck the output/ directory for detailed results:")
        print("  - network_edges.csv: Sponsor-originator relationships")
        print("  - originator_centrality.csv: Originator centrality metrics")
        print("  - sponsor_centrality.csv: Sponsor centrality metrics")
        print("  - originator_projection.csv: Originator-originator relationships")
        print("  - sponsor_projection.csv: Sponsor-sponsor relationships")
        
    except Exception as e:
        print(f"\nError during network analysis: {e}")
        print("Make sure you have run the previous examples to set up the data.")


if __name__ == "__main__":
    main()
