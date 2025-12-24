import streamlit as st
import hashlib
import json
import networkx as nx
import matplotlib.pyplot as plt
import time

def sha256_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def load_8_reviews(path):
    leaf_hashes = []
    ids = []
    with open(path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if idx == 8:
                break
            data = json.loads(line)
            rid = (
                data.get("reviewID") or
                data.get("reviewerID") or
                data.get("id") or
                str(idx)
            )
            asin = data.get("asin", "")
            rating = str(data.get("overall", ""))
            text = data.get("reviewText", "").strip()
            combined = rid + "|" + asin + "|" + rating + "|" + text
            leaf_hashes.append(sha256_hash(combined))
            ids.append(rid)
    return leaf_hashes, ids



def build_merkle_tree(leaf_hashes):
    G = nx.DiGraph()
    layers = []
    node_labels = {}

    current = leaf_hashes[:]
    layers.append(current)

    while len(current) > 1:
        next_layer = []
        for i in range(0, len(current), 2):
            parent = sha256_hash(current[i] + current[i+1])
            next_layer.append(parent)
        layers.append(next_layer)
        current = next_layer

   
    for lvl, layer in enumerate(layers):
        for i, h in enumerate(layer):
            node_id = f"{lvl}_{i}"
            G.add_node(node_id)
            node_labels[node_id] = h

   
    for lvl in range(1, len(layers)):
        for i in range(len(layers[lvl])):
            parent = f"{lvl}_{i}"
            left  = f"{lvl-1}_{2*i}"
            right = f"{lvl-1}_{2*i+1}"
            G.add_edge(parent, left)
            G.add_edge(parent, right)

    return G, layers, node_labels, layers[-1][0]


def tree_layout(layers):
    pos = {}
    y_gap = 2
    for level, layer in enumerate(layers):
        count = len(layer)
        x_spacing = 1.8
        start_x = -(count - 1) / 2 * x_spacing
        for i in range(count):
            x = start_x + i * x_spacing
            y = -level * y_gap
            pos[f"{level}_{i}"] = (x, y)
    return pos


def draw_tree(G, pos, node_labels, highlight=None):
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = []
    for node in G.nodes():
        if highlight and node in highlight:
            colors.append("red")
        else:
            colors.append("skyblue")
    nx.draw(G, pos, node_color=colors, node_size=1600, with_labels=False, ax=ax)
    for node, (x, y) in pos.items():
        short = node_labels[node][:6] + "..."
        ax.text(x, y, short, ha="center", va="center", fontsize=8)
    return fig

def generate_proof(layers, index):
    proof = []
    idx = index
    for lvl in range(len(layers)-1):

        if idx % 2 == 0:
            sibling = idx + 1
        else:
            sibling = idx - 1

        proof.append((lvl, idx, sibling))
        idx //= 2

    return proof


def main():
    st.title("Merkle Tree Visualization")
    leaf_hashes, ids = load_8_reviews("Movies_and_TV_5.json")
    G, layers, node_labels, root = build_merkle_tree(leaf_hashes)
    pos = tree_layout(layers)

    st.subheader("Merkle Tree for 8 reviews")
    st.pyplot(draw_tree(G, pos, node_labels))

    st.subheader("Generate Merkle Proof Path")
    index = st.number_input("Select leaf index (0–7)", min_value=0, max_value=7, step=1)

    if st.button("Show Animated Proof Path"):
        proof = generate_proof(layers, index)
        animated = []

        for (lvl, idx, sibling) in proof:

            animated.append(f"{lvl}_{idx}")

            animated.append(f"{lvl}_{sibling}")

            fig = draw_tree(G, pos, node_labels, highlight=animated)
            st.pyplot(fig)
            time.sleep(0.7)


if __name__ == "__main__":
    main()
