import argparse
import json
import sys

import faiss
import numpy as np

from preparation.logger import logger


def load_embeddings(input_file):
    res = []
    with open(input_file, "r") as f:
        for line in f:
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

    logger.info(f"Input dir    : {args.input}")
    logger.info(f"Output file  : {args.output}")

    data = load_embeddings(args.input)
    logger.info(f"Loaded {len(data)} embeddings")
    embeddings = np.stack([d["emb"] for d in data])
    logger.info(f"Before norms mean: {np.mean(np.linalg.norm(embeddings, axis=1)):.4f}")

    faiss.normalize_L2(embeddings)
    logger.info(f"Norms mean: {np.mean(np.linalg.norm(embeddings, axis=1)):.4f}")
    logger.info(f"Normalized {embeddings.shape}")

    d = embeddings.shape[1] 
    nlist = 100  # Number of Voronoi cells (approx. clusters)
    quantizer = faiss.IndexFlatIP(d)  # Inner product (cosine similarity)
    index = faiss.IndexIVFFlat(quantizer, d, nlist, faiss.METRIC_INNER_PRODUCT)

    logger.info(f"Training...")
    index.train(embeddings)
    index.add(embeddings)

    distances, indices = index.search(embeddings, 50)

    similarity_threshold = 0.75
    edges = []

    for i in range(len(embeddings)):
        for j, score in zip(indices[i], distances[i]):
            if i == j:
                continue  # skip self
            if score >= similarity_threshold:
                edges.append((i, j))

    uf = UnionFind(len(embeddings))
    for i, j in edges:
        uf.union(i, j)

    clusters = uf.get_clusters()
    logger.info(f"Cluster completed")

    res = []
    for cluster in clusters:
        if len(cluster) < 2:
            continue
        logger.info(f"Cluster with {len(cluster)} embeddings")
        sp = []
        for idx in cluster:
            d = data[idx]
            sp.append({"sp": d["sp"], "f": d["f"]})
        res.append(sp)

    with open(args.output, "w") as f:
        for cluster in res:
            f.write(json.dumps(cluster) + "\n")

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
