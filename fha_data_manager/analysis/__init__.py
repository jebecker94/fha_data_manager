"""Analysis modules for FHA data."""

from .exploratory import (
    load_combined_data,
    analyze_lender_activity,
    analyze_sponsor_activity,
    analyze_loan_characteristics,
    plot_active_lenders_over_time,
    plot_average_loan_size_over_time,
    plot_loan_purpose_distribution,
    plot_purchase_and_refinance_trend,
    plot_down_payment_source_trend,
    plot_interest_rate_by_product_type,
    plot_interest_rate_by_property_type,
    plot_interest_rate_by_loan_purpose,
    plot_loan_amount_by_loan_purpose,
    create_all_trend_plots,
)
from .institutions import InstitutionAnalyzer
from .network import (
    analyze_sponsor_originator_network,
    build_bipartite_graph,
    compute_centrality_metrics,
    load_originator_sponsor_edges,
    project_affiliation_graphs,
)

__all__ = [
    "load_combined_data",
    "analyze_lender_activity",
    "analyze_sponsor_activity",
    "analyze_loan_characteristics",
    "plot_active_lenders_over_time",
    "plot_average_loan_size_over_time",
    "plot_loan_purpose_distribution",
    "plot_purchase_and_refinance_trend",
    "plot_down_payment_source_trend",
    "plot_interest_rate_by_product_type",
    "plot_interest_rate_by_property_type",
    "plot_interest_rate_by_loan_purpose",
    "plot_loan_amount_by_loan_purpose",
    "create_all_trend_plots",
    "analyze_sponsor_originator_network",
    "build_bipartite_graph",
    "compute_centrality_metrics",
    "load_originator_sponsor_edges",
    "project_affiliation_graphs",
    "InstitutionAnalyzer",
]

