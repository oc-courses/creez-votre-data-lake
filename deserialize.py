import json
import os
import sys

import fastavro
import hdfs

src_file = sys.argv[1]

hdfs_client = hdfs.InsecureClient("http://0.0.0.0:50070")
schema = json.load(open(os.path.join(os.path.dirname(__file__), "node.avsc")))

with hdfs_client.read(src_file) as avro_file:
    reader = fastavro.reader(avro_file, reader_schema=schema)
    for node in reader:
        print(node)
