"""
This application extracts autoscaler metrics and dumps them into a CSV file
"""
import argparse
import json
import csv
import re

from google.cloud import logging

def list_entries(logger_name, clusterName):
    """Lists the most recent entries for a given logger."""
    logging_client = logging.Client()
    logger = logging_client.logger(logger_name)

    print("Writing autoscaler metrics into memory_metrics.csv")
    rows = []
    for entry in logger.list_entries():
        obj=json.loads(json.dumps(entry.payload))
        labels = json.loads(json.dumps(entry.resource.labels))
        cluster_name=labels["cluster_name"]
        if clusterName==cluster_name:
            if obj["status"]["state"]=="RECOMMENDING":
                pending_memory = obj["recommendation"]["inputs"]["clusterMetrics"]["avg-yarn-pending-memory"]
                available_memory = obj["recommendation"]["inputs"]["clusterMetrics"]["avg-yarn-available-memory"]
                metrics=[pending_memory, available_memory]
                metrics = re.findall('\d+', ' '.join(metrics))
                rows.append(metrics)
        
    with open('memory_metrics.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['pending_memory', 'available_memory'])
        writer.writerows(rows)


if __name__ == "__main__":
    logName="dataproc.googleapis.com%2Fautoscaler"
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("clusterName", help="Cluster name", default="poc-cluster")
    subparsers = parser.add_subparsers(dest="clusterName")
    args = parser.parse_args()
   
    list_entries(logName, args.clusterName)