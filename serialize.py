import json
import os
import xml.etree.ElementTree as ET
import sys

import fastavro
import hdfs

def main():
    # Read command line arguments
    src_dir = sys.argv[1]
    dst_dir = sys.argv[2]

    # Read schema
    schema = json.load(open(os.path.join(os.path.dirname(__file__), "node.avsc")))

    # create HDFS client
    hdfs_client = hdfs.InsecureClient("http://0.0.0.0:50070")

    # List files
    for filename in hdfs_client.list(src_dir):
        if os.path.splitext(filename)[1] != ".osm":
            continue
        osm_path = os.path.join(src_dir, filename)
        avro_path = os.path.join(dst_dir, filename[:-4] + ".avro")

        # Skip file if destination file already exists
        if hdfs_client.status(avro_path, strict=False) is not None:
            print("Skipping ", avro_path)
            continue

        print("Processing ", avro_path)
        try:
            serialize(osm_path, avro_path, hdfs_client, schema)
        except:
            # Delete destination file if it was created
            hdfs_client.delete(avro_path)
            raise

def serialize(osm_path, avro_path, hdfs_client, schema):
    with hdfs_client.read(osm_path) as osm_file:
        tree = ET.parse(osm_file)
        nodes = []
        for node in tree.iterfind("node"):
            id = int(node.get("id"))
            longitude = float(node.get("lon"))
            latitude = float(node.get("lat"))
            username = node.get("user")
            tags = {
                tag.get("k"): tag.get("v") for tag in node.iterfind("tag")
            }
            nodes.append({
                "id": id,
                "longitude": longitude,
                "latitude": latitude,
                "username": username,
                "tags": tags
            })

    with hdfs_client.write(avro_path) as avro_file:
        fastavro.writer(avro_file, schema, nodes)

if __name__ == "__main__":
    main()
