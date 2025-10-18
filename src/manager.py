"""
Manager class is responsible for managing Kubernetes resources and orchestrating the setup and execution
of a trace emulation process. It interacts with the Kubernetes API to manage namespaces, deployments,
nodeclaims, and other resources, while also handling custom logic for scaling and disruption times.
"""

import subprocess
import time
from pods_mapping import PodsMapping
from collector import Collector
from workload.manager import WorkloadManager
from infrastructure.instance_preemption import PreemptionManager
from infrastructure.manager import InfrastructureManager
import threading

class Manager:
    """
    Manager class is responsible for managing Kubernetes resources and orchestrating the setup and execution
    of a trace emulation process. It interacts with the Kubernetes API to manage namespaces, deployments,
    nodeclaims, and other resources, while also handling custom logic for scaling and disruption times.
    Attributes:
        input_step (int): Time step interval for processing trace entries.
        data_path (string): The path of the JSON with the objects that will be applied by broker.
    """

    def __init__(self, data_path='/tmp', karpenter=True, infrastructure=None, workload=None, skip_pods_mapping=False, emulation_name=None, speed_up_factor=None, use_interruption_model=False, interruption_rate=None, node_interruption_interval=300, interruption_random_seed=42):
        """
        Initializes the Manager class.
        """
        self.infrastructure = infrastructure
        self.workload = workload
        self.emulation_name = emulation_name

        infrastructure_event = threading.Event()
        workload_event = threading.Event()

        self.infrastructure_manager = InfrastructureManager(f"{data_path}/infrastructure_description.json", karpenter, infrastructure, infrastructure_event, speed_up_factor)
        self.workload_manager = WorkloadManager(f"{data_path}/workload_description.json", workload, workload_event, speed_up_factor)

        if use_interruption_model:
            self.preemption_manager = PreemptionManager(interruption_rate, f"{data_path}/workload_description.json",  interruptions_interval=node_interruption_interval, infrastructure_event=infrastructure_event, workload_event=workload_event, seed=interruption_random_seed)

        self.pods_mapping = PodsMapping(karpenter)
        self.collector = Collector(step=15, emulation_name=emulation_name)

        self.skip_pods_mapping = skip_pods_mapping

        self.speed_up_factor = speed_up_factor

    def log(self, message):
        """
        Logs a message with a "[BROKER]" prefix.
        """
        print(f"[BROKER] {message}")

    def start_mapping_and_scheduler(self):
        """
        Starts the process of mapping pods and running the scheduler.

        This method performs the following actions:
        1. Executes the `run` method of the `pods_mapping` object to initiate pod mapping.
        2. Runs the `build-scheduler.sh` script using a subprocess call to set up the scheduler.
        """
        self.pods_mapping.run()
        subprocess.run(["bash", "src/build-scheduler.sh"], check=True)

    def run(self):
        """
        Executes the main workflow of the Broker.

        This method orchestrates the entire lifecycle of the Broker's operation.
        """
        self.log("[INFO] Starting Broker.")
        self.log("[INFO] Preparing to start emulation.")
        self.infrastructure_manager.before_setup()
        self.workload_manager.before_setup()

        self.log("[INFO] Executing setup of infrastructure and workload.")
        self.infrastructure_manager.setup()
        self.workload_manager.setup()

        if not self.skip_pods_mapping:
            self.start_mapping_and_scheduler()

        self.workload_manager.before_emulation()
        self.infrastructure_manager.before_emulation()

        self.log("[INFO] Starting emulation.")
        start = int(time.time())

        with open("/home/ubuntu/emulation_time.txt", "a") as f:
            f.write(f"----- {self.emulation_name} -----\n")
            f.write(f"Start time: {start}\n")

        # 1. Criar as threads para os métodos de emulação
        infra_emulation_thread = threading.Thread(
            target=self.infrastructure_manager.emulation,
            name="InfraEmulationThread"
        )
        workload_emulation_thread = threading.Thread(
            target=self.workload_manager.emulation,
            name="WorkloadEmulationThread"
        )
        interruption_thread = threading.Thread(
            target=self.preemption_manager.emulation,
            name="InterruptionThread"
        )

        self.log("[INFO] Starting emulation thread for InfrastructureManager.")
        infra_emulation_thread.start()
        self.log("[INFO] Starting emulation thread for WorkloadManager.")
        workload_emulation_thread.start()
        self.log("[INFO] Starting emulation thread for PreemptionManager.")
        interruption_thread.start()
    
        infra_emulation_thread.join()
        self.log("[INFO] InfrastructureManager emulation thread completed.")
        workload_emulation_thread.join()
        self.log("[INFO] WorkloadManager emulation thread completed.")
        interruption_thread.join()
        self.log("[INFO] PreemptionManager emulation thread completed.")

        end = int(time.time())

        with open("/home/ubuntu/emulation_time.txt", "a") as f:
            f.write(f"End time: {end}\n")
            f.write(f"Duration: {end - start} seconds\n")

        duration = end - start 
        subprocess.run(["bash", "src/port-forward.sh"], check=True)
        self.collector.collect(start_time=start, end_time=end, duration=duration)

        self.log("[INFO] Emulation completed. Tearing down infrastructure, workload and temp files.")
        self.infrastructure_manager.tear_down()
        self.workload_manager.tear_down()