"""
Network / Link Analysis
========================
Build communication graphs, compute centrality metrics, detect communities,
and simulate network dismantling to prioritize investigative targets.
"""

import pandas as pd
import networkx as nx
from collections import defaultdict


def build_graph(df: pd.DataFrame) -> nx.DiGraph:
    """
    Build a directed weighted communication graph from CDR data.

    Nodes  = phone numbers
    Edges  = calls from caller → callee
    Weight = number of calls on that edge

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned CDR DataFrame.

    Returns
    -------
    nx.DiGraph
        Directed graph with edge attribute 'weight' (call count)
        and 'duration' (total seconds).
    """
    G = nx.DiGraph()

    edge_calls = defaultdict(int)
    edge_duration = defaultdict(int)

    for _, row in df.iterrows():
        src = row["Calling_Number"]
        dst = row["Called_Number"]
        edge_calls[(src, dst)] += 1
        edge_duration[(src, dst)] += int(row["Duration_sec"])

    for (src, dst), count in edge_calls.items():
        G.add_edge(src, dst, weight=count, duration=edge_duration[(src, dst)])

    return G


def compute_centrality(G: nx.DiGraph) -> pd.DataFrame:
    """
    Compute multiple centrality metrics for each node.

    Metrics
    -------
    - degree_centrality     : fraction of nodes this node is connected to
    - in_degree             : number of unique callers to this number
    - out_degree            : number of unique numbers this node called
    - betweenness_centrality: how often this node lies on shortest paths (broker/intermediary score)
    - pagerank              : influence score (weighted by network importance of callers)
    - call_count_out        : total outbound calls
    - call_count_in         : total inbound calls

    Parameters
    ----------
    G : nx.DiGraph

    Returns
    -------
    pd.DataFrame
        One row per node, sorted by betweenness_centrality descending.
    """
    undirected = G.to_undirected()

    degree_cent = nx.degree_centrality(undirected)
    betweenness = nx.betweenness_centrality(G, weight="weight", normalized=True)
    pagerank = nx.pagerank(G, weight="weight")

    rows = []
    for node in G.nodes():
        out_edges = G.out_edges(node, data=True)
        in_edges = G.in_edges(node, data=True)
        call_out = sum(d["weight"] for _, _, d in out_edges)
        call_in = sum(d["weight"] for _, _, d in in_edges)

        rows.append({
            "phone_number": node,
            "degree_centrality": round(degree_cent.get(node, 0), 4),
            "in_degree": G.in_degree(node),
            "out_degree": G.out_degree(node),
            "betweenness_centrality": round(betweenness.get(node, 0), 4),
            "pagerank": round(pagerank.get(node, 0), 4),
            "call_count_out": call_out,
            "call_count_in": call_in,
            "total_calls": call_out + call_in,
        })

    return pd.DataFrame(rows).sort_values("betweenness_centrality", ascending=False).reset_index(drop=True)


def detect_communities(G: nx.DiGraph) -> dict:
    """
    Detect criminal sub-groups using greedy modularity community detection.

    Works on the undirected version of the graph. Falls back to connected
    components if the graph has fewer than 3 nodes.

    Parameters
    ----------
    G : nx.DiGraph

    Returns
    -------
    dict
        Mapping of phone_number → community_id (int, 0-indexed).
    """
    undirected = G.to_undirected()

    if len(undirected.nodes) < 3:
        # Trivially assign all to community 0
        return {n: 0 for n in undirected.nodes}

    communities = nx.community.greedy_modularity_communities(undirected, weight="weight")

    node_to_community = {}
    for community_id, community_set in enumerate(communities):
        for node in community_set:
            node_to_community[node] = community_id

    return node_to_community


def simulate_dismantling(G: nx.DiGraph, top_n: int = 5) -> list[dict]:
    """
    Simulate network dismantling by iteratively removing the highest-betweenness node.

    Shows how removing key suspects degrades network connectivity.
    Used to prioritize who to arrest first.

    Parameters
    ----------
    G : nx.DiGraph
        Original communication graph.
    top_n : int
        Number of removal steps to simulate.

    Returns
    -------
    list of dict
        Each step: removed node, remaining edges, largest component size,
        and network efficiency (0–1).
    """
    G_sim = G.copy()
    results = []

    for step in range(min(top_n, len(G.nodes) - 1)):
        if len(G_sim.nodes) == 0:
            break

        betweenness = nx.betweenness_centrality(G_sim, weight="weight", normalized=True)
        if not betweenness:
            break

        target = max(betweenness, key=betweenness.get)
        G_sim.remove_node(target)

        undirected = G_sim.to_undirected()
        components = list(nx.connected_components(undirected))
        largest = max((len(c) for c in components), default=0)

        # Network efficiency: average inverse shortest path length
        efficiency = nx.global_efficiency(undirected) if len(G_sim.nodes) > 1 else 0.0

        results.append({
            "step": step + 1,
            "removed_node": target,
            "remaining_nodes": len(G_sim.nodes),
            "remaining_edges": len(G_sim.edges),
            "largest_component_size": largest,
            "network_efficiency": round(efficiency, 4),
        })

    return results


def get_graph_stats(G: nx.DiGraph) -> dict:
    """Return high-level graph statistics."""
    undirected = G.to_undirected()
    components = list(nx.connected_components(undirected))

    return {
        "nodes": len(G.nodes),
        "edges": len(G.edges),
        "density": round(nx.density(G), 4),
        "connected_components": len(components),
        "largest_component_size": max((len(c) for c in components), default=0),
        "avg_clustering": round(nx.average_clustering(undirected), 4),
    }
