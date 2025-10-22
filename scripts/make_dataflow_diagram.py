from graphviz import Digraph

g = Digraph("dataflow", format="png")
g.attr("node", shape="box", style="rounded", fontsize="11")

g.edge("NYC Parquet 2025-01..03", "Local temp", label="download_to_temp")
g.edge("Local temp", "GCS raw", label="upload_to_gcs.py")
g.edge("GCS raw", "Trips DF", label="read parquet (etl_revenue_per_day.py)")
g.edge("Trips DF", "Revenue/day DF", label="groupby pickup_date + sum(total_amount)")
g.edge("Revenue/day DF", "CSV local", label="to_csv")
g.edge("CSV local", "GCS results", label="upload CSV")

g.render("docs/dataflow", cleanup=True)
print("✅ Wrote docs/dataflow.png")
