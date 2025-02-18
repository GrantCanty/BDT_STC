from kafka import KafkaConsumer
import json
import networkx as nx
import matplotlib.pyplot as plt

KAFKA_BROKER = "kafka:9092"
import socket
try:
    socket.create_connection(('kafka', 9092), timeout=10)
    print("Kafka is reachable!")
except socket.error as e:
    print(f"Kafka is not reachable: {e}")

def visualize_graph(new_graph, routes):
    G = nx.Graph()
    for node in new_graph["nodes"]:
        G.add_node(node["id"], **node)
    for link in new_graph["links"]:
        G.add_edge(link["source"], link["target"], weight=link["weight"])
    pos = nx.spring_layout(G)  # Layout for visualization
    plt.figure(figsize=(12, 8))
    nx.draw(G, pos, with_labels=True, node_size=500, node_color="lightblue")
    labels = nx.get_edge_attributes(G, "weight")
    nx.draw_networkx_edge_labels(G, pos, edge_labels=labels)
    for garage, data in routes.items():
        for node, path in data["paths"].items():
            edges = list(zip(path, path[1:]))
            nx.draw_networkx_edges(G, pos, edgelist=edges, edge_color="red", width=2)

    plt.title("Optimal Routes Visualization")
    plt.show()


def consume_from_kafka():
    consumer = KafkaConsumer(
        "graph",
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    for message in consumer:
        data = message.value
        new_graph = data["new_graph"]
        routes = data["links"]
        visualize_graph(new_graph, routes)

