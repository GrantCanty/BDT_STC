import json
from kafka import KafkaConsumer, KafkaProducer

import networkx as nx

import socket
try:
    socket.create_connection(('kafka', 9092), timeout=10)
    print("Kafka is reachable!")
except socket.error as e:
    print(f"Kafka is not reachable: {e}")
# Kafka configuration
KAFKA_TOPIC = 'collect_bins'
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

# Path to the graph.json file
GRAPH_JSON_PATH = '/app/newgraph.json'
NEW_GRAPH_JSON_PATH = '/app/updated_graph.json'

def consume_sensor_ids():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='sensor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    sensor_ids = set()

    for message in consumer:
        data = message.value  
        print(f"Received Kafka message: {data}") 

        if isinstance(data, dict) and "sensor_ids" in data:  
            sensor_ids.update(data["sensor_ids"]) 
        else:
            print(f"Unexpected message format: {data}")  
        if sensor_ids:
            break  # Stop after the first valid message

    consumer.close()
    return sensor_ids


def filter_graph(sensor_ids):
    with open(GRAPH_JSON_PATH, 'r') as file:
        graph = json.load(file)

    # Assuming graph['nodes'] is a list of dictionaries
    filtered_nodes =[
        node for node in graph['nodes']
        if node['id'] in sensor_ids or node['id'] > 199
    ]
    filtered_ids = {node['id'] for node in filtered_nodes}
    new_graph = {
        'nodes': filtered_nodes,
        'links': [
            link for link in graph.get('links', [])
            if link['source'] in filtered_ids and link['target'] in filtered_ids
        ]
    }
    with open(NEW_GRAPH_JSON_PATH, 'w') as file:
        json.dump(new_graph, file, indent=4)
    return new_graph
def compute_routes(new_graph):
    """
    Compute optimal routes for waste collection, selecting an optimal subset of garages.
    """
    G = nx.Graph()

    # Add nodes
    for node in new_graph['nodes']:
        G.add_node(node['id'], **node)

    # Add edges with weights (distances)
    for link in new_graph['links']:
        G.add_edge(link['source'], link['target'], weight=link['weight'])

    # Identify garage nodes
    garages = [n for n, data in G.nodes(data=True) if data['point_type'] == 'garage']
    
    if not garages:
        raise ValueError("No garages found in the graph!")

    # Identify waste collection points
    waste_bins = [n for n, data in G.nodes(data=True) if data['point_type'] == 'waste_bin']

    # Step 1: Compute shortest paths from each garage to all nodes
    all_routes = {}
    for garage in garages:
        shortest_paths = nx.single_source_dijkstra_path(G, garage, weight='weight')
        shortest_distances = nx.single_source_dijkstra_path_length(G, garage, weight='weight')
        all_routes[garage] = {
            'paths': shortest_paths,
            'distances': shortest_distances
        }

    # Step 2: Select garages efficiently (heuristic approach)
    selected_garages = set()
    assigned_nodes = set()

    while waste_bins:
        # Pick the garage that covers the most unassigned bins with the shortest distance
        best_garage = None
        best_coverage = 0
        best_total_distance = float('inf')

        for garage in garages:
            if garage in selected_garages:
                continue

            covered_nodes = [n for n in waste_bins if n in all_routes[garage]['distances']]
            total_distance = sum(all_routes[garage]['distances'].get(n, float('inf')) for n in covered_nodes)

            if len(covered_nodes) > best_coverage or (len(covered_nodes) == best_coverage and total_distance < best_total_distance):
                best_garage = garage
                best_coverage = len(covered_nodes)
                best_total_distance = total_distance

        if not best_garage:
            break  # No garage can cover remaining bins, exit loop

        selected_garages.add(best_garage)
        assigned_nodes.update(all_routes[best_garage]['distances'].keys())
        waste_bins = [n for n in waste_bins if n not in assigned_nodes]

    # Step 3: Generate routes for selected garages
    final_routes = {}
    for garage in selected_garages:
        final_routes[garage] = {
            'paths': {n: all_routes[garage]['paths'][n] for n in all_routes[garage]['paths'] if n in assigned_nodes},
            'distances': {n: all_routes[garage]['distances'][n] for n in all_routes[garage]['distances'] if n in assigned_nodes}
        }

    return final_routes

KAFKA_BROKER = "kafka:9092" 

def send_to_kafka(new_graph, routes):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    message = {
        "new_graph": new_graph,
        "routes": routes
    }

    producer.send("graph", message)
    producer.flush()
    print("Sent graph and routes to Kafka topic 'graph'.")



def main():
    sensor_ids = consume_sensor_ids()

    print(sensor_ids)
    new_graph=filter_graph(sensor_ids)
    print(f"Filtered graph written to {NEW_GRAPH_JSON_PATH}")
    routes=compute_routes(new_graph)
    send_to_kafka(new_graph, routes)

if __name__ == '__main__':
    main()