from kafka import KafkaConsumer
import json
import networkx as nx
import matplotlib.pyplot as plt


KAFKA_TOPIC = "graph"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
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
    output_path = "/app/graph_visualization.png" 
    plt.savefig(output_path)
    print(f"Graph saved to {output_path}")

    plt.close()

def consume_graph():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="graph-visualization",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print("Listening for messages on Kafka topic:", KAFKA_TOPIC)
    for message in consumer:
        data = message.value  # Extract the Kafka message payload
        print(data)
        # Ensure the structure is correct
        new_graph = data.get("new_graph", {})  # Get the graph
        routes = data.get("routes", {})  # Get the routes

        nodes = new_graph.get("nodes", [])  # Extract nodes
        links = new_graph.get("links", [])  # Extract links

        # Debugging
        print(f"Received {len(nodes)} nodes and {len(links)} links from Kafka.")

        # Now visualize the graph
        visualize_graph(new_graph, routes)
    consumer.close()
if __name__ == "__main__":
    consume_graph()
