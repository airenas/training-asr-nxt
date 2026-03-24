import argparse
import json
import sys

import faiss
import numpy as np
from tqdm import tqdm
import igraph as ig
import leidenalg

from preparation.logger import logger


def load_embeddings(input_file):
    res = []
    with open(input_file, "r") as f:
        for line in tqdm(f, desc="Loading embeddings"):
            record = json.loads(line)
            record["emb"] = np.array(record["emb"], dtype=np.float32)
            res.append(record)
    return res


class UnionFind:
    def __init__(self, n):
        self.parent = list(range(n))

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression
            x = self.parent[x]
        return x

    def union(self, x, y):
        self.parent[self.find(x)] = self.find(y)

    def get_clusters(self):
        clusters = {}
        for i in range(len(self.parent)):
            root = self.find(i)
            clusters.setdefault(root, []).append(i)
        return list(clusters.values())


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Cluster all embeddings")
    parser.add_argument("--input", nargs='?', required=True, help="Input file")
    parser.add_argument("--output", nargs='?', required=True, help="File")
    args = parser.parse_args(args=argv)

    nprobe = 96
    k = 200
    nlist = 4096
    similarity_threshold = 0.80
    top_k = 50

    logger.info(f"Input dir    : {args.input}")
    logger.info(f"Output file  : {args.output}")

    logger.info(f"nprobe : {nprobe}")
    logger.info(f"k      : {k}")
    logger.info(f"nlist  : {nlist}")
    logger.info(f"sim thr: {similarity_threshold}")
    logger.info(f"top_k  : {top_k}")


    data = load_embeddings(args.input)
    logger.info(f"Loaded {len(data)} embeddings")
    embeddings = np.stack([d["emb"] for d in data])
    logger.info(f"Before norms mean: {np.mean(np.linalg.norm(embeddings, axis=1)):.4f}")

    faiss.normalize_L2(embeddings)
    logger.info(f"Norms mean: {np.mean(np.linalg.norm(embeddings, axis=1)):.4f}")
    logger.info(f"Normalized {embeddings.shape}")

    d = embeddings.shape[1]
    quantizer = faiss.IndexFlatIP(d)  # Inner product (cosine similarity)
    index = faiss.IndexIVFFlat(quantizer, d, nlist, faiss.METRIC_INNER_PRODUCT)

    logger.info(f"Training...")
    index.train(embeddings)
    logger.info(f"Training completed. Adding embeddings...")
    index.add(embeddings)

    index.nprobe = nprobe

    logger.info(f"Searching for similar pairs...")
    distances, indices = index.search(embeddings, k)

    # neighbors = [dict() for _ in range(len(embeddings))]
    # for i in tqdm(range(len(embeddings)), desc="Building mutual kNN graph"):
    #     for j, score in zip(indices[i][:top_k], distances[i][:top_k]):
    #         if i == j:
    #             continue
    #         if score >= similarity_threshold:
    #             neighbors[i][j] = score

    edges = []
    for i in tqdm(range(len(embeddings)), desc="Building kNN edges"):
        for j, score in zip(indices[i][:top_k], distances[i][:top_k]):
            if i == j:
                continue
            if score >= similarity_threshold:
                edges.append((i, j, float(score)))

    # for i in tqdm(range(len(embeddings)), desc="Building mutual edges"):
    #     for j, score in neighbors[i].items():
    #         if i in neighbors[j]:
    #             w = (score + neighbors[j][i]) / 2.0
    #             edges.append((i, j, float(w)))

    seen = set()
    clean_edges = []

    for i, j, w in edges:
        if i > j:
            i, j = j, i
        if (i, j) not in seen:
            seen.add((i, j))
            clean_edges.append((i, j, w))

    edges = clean_edges

    logger.info(f"Found {len(edges)} similar pairs")

    logger.info("Building graph for Leiden...")

    g = ig.Graph()
    g.add_vertices(len(embeddings))

    g.add_edges([(i, j) for i, j, _ in edges])
    g.es["weight"] = [w for _, _, w in edges]

    logger.info("Running Leiden clustering...")

    partition = leidenalg.find_partition(
        g,
        leidenalg.RBConfigurationVertexPartition,
        weights="weight",
        resolution_parameter=1.0
    )

    clusters = [[] for _ in range(max(partition.membership) + 1)]

    for node_id, cluster_id in enumerate(partition.membership):
        clusters[cluster_id].append(node_id)

    logger.info("Leiden clustering completed")

    # neighbors = [set(row[:top_k]) for row in indices]
    # for i in tqdm(range(len(embeddings)), desc="Finding similar pairs", total=len(embeddings)):
    #     for j, score in zip(indices[i][:top_k], distances[i][:top_k]):
    #         if i == j:
    #             continue  # skip self
    #         if score >= similarity_threshold and i in neighbors[j]:  # mutual nearest neighbors
    #             edges.append((i, j))


    # logger.info(f"Union-Find clustering...")
    # uf = UnionFind(len(embeddings))
    # for i, j in edges:
    #     uf.union(i, j)
    #
    # clusters = uf.get_clusters()
    # logger.info(f"Cluster completed")

    sizes = sorted([len(c) for c in clusters], reverse=True)

    cl_size = 30
    logger.info(f"Top {cl_size} cluster sizes: {sizes[:cl_size]}")
    logger.info(f"Total clusters: {len(clusters)}")
    logger.info(f"Singleton clusters: {sum(1 for c in clusters if len(c) == 1)}")

    res = []
    for cluster in tqdm(clusters, desc="Processing clusters"):
        # if len(cluster) < 2:
        #     continue
        logger.debug(f"Cluster with {len(cluster)} embeddings")
        sp = []
        for idx in cluster:
            d = data[idx]
            sp.append({"sp": d["sp"], "f": d["f"]})
        res.append(sp)

    with open(args.output, "w") as f:
        for cluster in tqdm(res, desc="Writing clusters"):
            f.write(json.dumps(cluster) + "\n")

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
