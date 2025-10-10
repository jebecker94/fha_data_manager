# Suggestions for FHA Data Research

- [ ] Build a lender-level panel dataset that extends `analyze_fha_data.analyze_lender_activity` by aggregating loan counts, total volume, average interest rates, and purchase-versus-refinance shares at annual or quarterly frequency, and export the result for econometric modeling (e.g., market share and Herfindahl analyses).
- [ ] Leverage the `import_fha_data.add_county_fips` enrichment to create county- and metro-level summary tables (loan counts, median mortgage amounts, interest rate dispersion) that researchers can merge with BLS/ACS indicators when studying local housing market dynamics.
- [ ] Expand `analyze_name_changes.analyze_name_changes` to emit a structured event log of lender/sponsor renamings and ownership transitions so researchers can run event studies on rebranding or consolidation episodes.
- [ ] Enhance `import_fha_data.create_lender_id_to_name_crosswalk` by capturing the first/last observed periods for each ID–name pairing and flagging conflicting names, enabling clean joins to regulatory and financial datasets.
- [ ] Combine insights from `check_common_ids.analyze_lender_relationships` and the sponsor/originator snapshots to build network analytics (e.g., bipartite graphs, centrality metrics) that illuminate how sponsor relationships influence FHA loan flows.
