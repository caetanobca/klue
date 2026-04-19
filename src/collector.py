"""
Collector class is responsible for collecting metrics from a Prometheus server over a specified duration.
It reads metric names from a file, queries Prometheus for the metrics, and writes the results to CSV files.
The CSV files are then compressed into a zip archive for easier handling.
"""

import csv
import time
import json
import os
import zipfile
import glob
from datetime import datetime, timedelta
import requests

class Collector:
    """
    This class collect and process metrics from a Prometheus server.

    Attributes:
        prometheus_host (str): The base URL of the Prometheus server.
        step (int): The step interval (in seconds) for querying metrics.
        metrics_file (str): The file containing the list of metrics to collect.
    """
    def __init__(self, step, metrics_file='src/metrics.txt', prometheus_host="http://localhost:30222", emulation_name=None):
        """
        Initializes the collector with the specified step interval, metrics file, 
        and Prometheus host URL.
        """
        self.prometheus_host = prometheus_host
        self.step = int(step)
        self.metrics_file = metrics_file
        self.emulation_name = emulation_name
        self.num_files = 80
        self.log_path = f"./logs/{emulation_name}" if emulation_name else "./logs/"

    def log(self, message):
        """
        Logs a message with a "[COLLECTOR]" prefix.
        """
        with open(f"{self.log_path}/collector.log", "a") as log_file:
            log_file.write(f"[COLLECTOR] {message}\n")

        print(f"[COLLECTOR] {message}")

    def read_metrics(self):
        """
        Reads metrics from a specified file and returns them as a list of strings.

        The method opens the file specified by `self.metrics_file` in read mode,
        reads each line, strips any leading or trailing whitespace, and stores
        the cleaned lines in a list.
        """
        with open(self.metrics_file, 'r') as f:
            metrics = [line.strip() for line in f]
        return metrics

    def request_metrics(self, metric, max_retries: int = 5, retry_delay: int = 2):
        """
        Fetches metrics from a Prometheus server within a specified time range, com retry e execução de comando bash.
        """

        if not self.step or self.step <= 0:
            self.log(f"[ERROR] Invalid step value: {self.step}. It must be a positive integer.")
            return None

        step_duration = f"{self.step}s"
        url = f"{self.prometheus_host}/api/v1/query_range"
        params = {
            "query": metric,
            "start": self.start_time,
            "end": self.end_time,
            "step": step_duration
        }

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, params=params)

                if response.status_code == 200:
                    return response
                else:
                    self.log(f"[WARNING] Attempt {attempt}: Failed to fetch metric {metric}: {response.text}")

            except requests.RequestException as e:
                self.log(f"[ERROR] Attempt {attempt}: Exception for metric {metric}: {e}")

            if attempt < max_retries:
                time.sleep(retry_delay)

        self.log(f"[ERROR] All {max_retries} attempts failed for metric {metric}.")

        return None

    def write_csv(self, output_dir, quartil = None, emulation_name=None):
        """
        Writes metrics data to CSV files in the specified output directory.

        This method iterates over a list of metrics, retrieves their data using
        the `request_metrics` method, and writes the results to individual CSV
        files. Each file is named after the metric's name and contains rows of
        timestamped values along with their associated labels.
        """
        self.log(f"[INFO] Writing CSV files to {output_dir}")
        for metric in self.metrics:
            response = self.request_metrics(metric)

            if response is None:
                continue

            try:
                results = response.json().get("data", {}).get("result", [])
            except json.JSONDecodeError:
                self.log(f"[ERROR] Failed to decode JSON for metric {metric}")
                with open(f"{self.log_path}/collector_failures.log", "a") as f:
                    f.write(f"Failed to decode JSON {metric} -- {quartil}\n")
                continue
            
            if len(results) > 0:
                metric_name = results[0]["metric"].get("__name__", "")
            else:
                self.log(f"[WARNING] No data found for metric {metric}")
                with open(f"{self.log_path}/collector_failures.log", "a") as f:
                    f.write(f"Len < 0 {metric} -- {quartil}\n")
                continue
            

            if quartil:
                metric_name = f"{metric_name}_{quartil}"

            if len(results) == 0:
                self.log(f"[INFO] No results for metric {metric}")
                with open(f"{output_dir}/{metric_name}.csv", "w", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    if emulation_name:
                        writer.writerow(["name", "timestamp", "value", "emulation_name"])
                    else:
                        writer.writerow(["name", "timestamp", "value"])
                continue

            with open(f"{output_dir}/{metric_name}.csv", "w", encoding="utf-8") as f:
                writer = csv.writer(f)
                labelnames = list(results[0]["metric"].keys())

                header = ["name", "timestamp", "value"] + labelnames
                if emulation_name:
                    header.append("emulation_name")

                writer.writerow(header)

                for result in results:
                    for values in result["values"]:
                        timestamp = values[0]
                        value = values[1]
                        row = [result["metric"].get("__name__", ""), timestamp, value]
                        for label in labelnames:
                            x = result["metric"].get(label, "")
                            row.append(x)
                        if emulation_name:
                            row.append(emulation_name)
                        writer.writerow(row)

    def collect(self, start_time, end_time, duration):
        """
        Collects metrics for a specified duration, writes them to CSV files, 
        and compresses the files into a ZIP archive.

        Steps:
            1. Logs the start of the metric collection process.
            2. Reads the metrics and stores them.
            3. Creates a timestamped output directory for the CSV files.
            4. Writes the collected metrics to CSV files in the output directory.
            5. Compresses the CSV files into a ZIP archive.
            6. Logs the completion of the zipping process.
        """
        self.log(f"[INFO] Collecting metrics of this emulation for {end_time - start_time} seconds.")
        self.start_time = start_time
        self.end_time = end_time
        now = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

        if self.emulation_name:
            output_dir = self.emulation_name
        else:
            output_dir = f"output_csv_{now}"
        
        
        os.makedirs(output_dir, exist_ok=True)

        metrics = self.read_metrics()
        self.log(f"[INFO] Metrics to be collected: {metrics}")
        for metric in metrics:
            self.log(f"[INFO] Metric to be collected: {metric}")
            self.metrics = [metric]

            self.log(f"[INFO] Collecting metric {metric} from {self.start_time} to {self.end_time}")

            self.start_time = start_time
            self.end_time = end_time
            for i in range(1, self.num_files + 1):
                percentil_time = start_time + (end_time - start_time) * (i / self.num_files)
                self.log(f"[INFO] Collecting file {i}/{self.num_files} from {self.start_time} to {percentil_time}")
                self.end_time = percentil_time
                self.write_csv(output_dir, quartil=i, emulation_name=self.emulation_name)
                self.start_time = percentil_time


        # Zip the CSV files
        # with zipfile.ZipFile(f"{output_dir}.zip", "w") as zip:
        #     for file in glob.glob(f"{output_dir}/*.csv"):
        #         zip.write(file)

        self.log(f"[INFO] CSV files zipped to {output_dir}.zip")
