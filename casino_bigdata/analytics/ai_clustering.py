import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg") 
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from pathlib import Path

BASE_DIR   = Path(r"C:\Users\caro_\OneDrive\Desktop\UP\1OCTAVO\datosMasivos\casino_bigdata")
OUTPUT_DIR = BASE_DIR / "data" / "exports"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PG_CONFIG = {
    "host"    : "localhost",
    "port"    : 5433,
    "dbname"  : "casino_db",
    "user"    : "postgres",
    "password": "postgres",
}

print("AI Component - K-Means Clustering")
print("grouping casino games by rtp, max_win, max_multiplier, min_bet")
print()

#load data
print("loading data from PostgreSQL...")
conn = psycopg2.connect(**PG_CONFIG)
df = pd.read_sql("""
    SELECT rtp, max_win, max_multiplier, min_bet, game_type, volatility
    FROM casino_games_raw
    WHERE rtp IS NOT NULL
      AND max_win IS NOT NULL
      AND max_multiplier IS NOT NULL
      AND min_bet IS NOT NULL
""", conn)
conn.close()
print(f"rows loaded: {len(df):,}")
print()

# prepare features 
features = ["rtp", "max_win", "max_multiplier", "min_bet"]
X = df[features].copy()

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
print("features scaled with StandardScaler")

#  k-means clustering 
N_CLUSTERS = 4
print(f"running K-Means with {N_CLUSTERS} clusters...")

kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10)
df["cluster"] = kmeans.fit_predict(X_scaled)

score = silhouette_score(X_scaled, df["cluster"], sample_size=10000, random_state=42)
print(f"silhouette score: {score:.4f}")
print()

# cluster summary 
print("cluster summary:")
print(f"  {'cluster':>8} | {'games':>8} | {'avg_rtp':>8} | {'avg_max_win':>12} | {'avg_multiplier':>15} | {'avg_min_bet':>12}")
print("  " + "-" * 75)

summary = df.groupby("cluster").agg(
    games          = ("rtp", "count"),
    avg_rtp        = ("rtp", "mean"),
    avg_max_win    = ("max_win", "mean"),
    avg_multiplier = ("max_multiplier", "mean"),
    avg_min_bet    = ("min_bet", "mean"),
).round(2)

labels = {
    summary["avg_rtp"].idxmax()       : "High RTP / Low Risk",
    summary["avg_max_win"].idxmax()   : "High Win Potential",
    summary["avg_multiplier"].idxmax(): "Mega Multiplier",
    summary["avg_min_bet"].idxmax()   : "High Stakes",
}
for i in range(N_CLUSTERS):
    if i not in labels:
        labels[i] = "Standard Games"

summary["label"] = summary.index.map(labels)

for idx, row in summary.iterrows():
    print(f"  {idx:>8} | {int(row.games):>8,} | {row.avg_rtp:>8.2f} | {row.avg_max_win:>12.2f} | {row.avg_multiplier:>15.2f} | {row.avg_min_bet:>12.4f}  <- {row.label}")

print()

#  plot 
print("generating cluster plot...")
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle("K-Means Clustering of Casino Games (k=4)", fontsize=14)

colors = ["#2196F3", "#4CAF50", "#FF9800", "#E91E63"]

# plot 1: rtp vs max_win
for c in range(N_CLUSTERS):
    subset = df[df["cluster"] == c].sample(min(3000, len(df[df["cluster"] == c])), random_state=42)
    axes[0].scatter(subset["rtp"], subset["max_win"],
                    c=colors[c], alpha=0.3, s=5,
                    label=f"Cluster {c}: {labels.get(c,'')}")
axes[0].set_xlabel("RTP (%)")
axes[0].set_ylabel("Max Win")
axes[0].set_title("RTP vs Max Win by Cluster")
axes[0].legend(fontsize=7)

# plot 2: cluster size bar chart
cluster_counts = df["cluster"].value_counts().sort_index()
cluster_labels = [f"C{i}\n{labels.get(i,'')}" for i in cluster_counts.index]
axes[1].bar(cluster_labels, cluster_counts.values, color=colors)
axes[1].set_xlabel("Cluster")
axes[1].set_ylabel("Number of Games")
axes[1].set_title("Games per Cluster")
for i, v in enumerate(cluster_counts.values):
    axes[1].text(i, v + 1000, f"{v:,}", ha="center", fontsize=9)

plt.tight_layout()
plot_path = OUTPUT_DIR / "kmeans_clusters.png"
plt.savefig(plot_path, dpi=150, bbox_inches="tight")
print(f"plot saved to: {plot_path}")
print()

out_csv = OUTPUT_DIR / "games_with_clusters.csv"
df[["rtp", "max_win", "max_multiplier", "min_bet",
    "game_type", "volatility", "cluster"]].to_csv(out_csv, index=False)
print(f"clustered data saved to: {out_csv}")
print()
print(f"silhouette score: {score:.4f}  (closer to 1.0 = better separation)")
print("clustering complete.")
