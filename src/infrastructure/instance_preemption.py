"""
This module defines the InfrastructureManager class, which manages the setup, emulation, and teardown of Kubernetes infrastructure
for trace-driven experiments. It handles applying Kubernetes objects, managing node pools, and coordinating emulation phases.
"""

import json
import time
from util.k8s_api.k8s_api import K8SAPI
from util.k8s_object_applier import KubernetesObjectApplier
import numpy as np
import pandas as pd


class PreemptionManager:
    TIME_OUT = 200
    AMOUNT_OF_REAL_NODES = 1

    def __init__(self, historical_data_path, workload_data_path,  interruptions_interval, infrastructure_event, workload_event, seed=None):
        """
        Initializes the Manager class.
        """

        self.k8s_api = K8SAPI(timeout=self.TIME_OUT)
        self.k8s_object_applier = KubernetesObjectApplier(self.k8s_api)

        self.data = pd.read_csv(historical_data_path)
        self.emulation_duration = 0
        with open(workload_data_path, 'r', encoding="utf-8") as file:
            temp_data = json.load(file)
            timestamps = [float(entry["timestamp"]) for entry in temp_data['emulation']]
            self.emulation_duration = (max(timestamps) - min(timestamps)) * 2
            print(f"Emulation duration: {self.emulation_duration}s")
            print(f"Total entries in workload: {len(temp_data['emulation'])}")
        self.interruptions_interval = interruptions_interval
        
        self.seed = seed if seed is not None else np.random.SeedSequence().entropy
        self.rng = np.random.default_rng(self.seed)

        self.infrastructure_event = infrastructure_event
        self.workload_event = workload_event

    def log(self, message):
        """
        Logs a message with a "[INFRASTRUCTURE MANAGER]" prefix.
        """
        print(f"[PREEMPTION MANAGER] {message}")


    def estimate_lambda_from_csv(self):
        """
        Estima λ = eventos / pessoa-tempo.
        - status=1 => preempção confirmada (evento)
        - status=0 => censura (conta tempo observado até 'emulation_duration')
        """
        lifetime_col = "lifetime"
        status_col = "status"
        emulation_duration = float(self.emulation_duration)

        data = self.data

        # limpeza mínima
        data = data.dropna(subset=[lifetime_col, status_col]).copy()
        data[lifetime_col] = data[lifetime_col].astype(float)
        data[status_col] = data[status_col].astype(int)

        # garantir positivos
        data = data[data[lifetime_col] > 0]
        if data.empty:
            raise ValueError("Sem linhas válidas (lifetimes > 0).")

        lifetimes = data[lifetime_col].to_numpy(dtype=float)
        status_val  = data[status_col].to_numpy(dtype=int)
        # (censura à direita no horizonte)
        print(emulation_duration)
        person_time = np.minimum(lifetimes, emulation_duration).sum()

        # número de eventos observados
        mask_events = (status_val == 1) & (lifetimes <= emulation_duration)
        events = int(mask_events.sum())


        if person_time <= 0:
            raise ValueError("Pessoa-tempo zero ou negativa após limpeza.")

        lam = events / person_time  # eventos por segundo
        return lam


    def prob_from_lambda(self, lam_per_sec) -> float:
        """
        Converte λ (1/s) para probabilidade por intervalo Δt: p = 1 - exp(-λΔt).
        """

        interruptions_interval = self.interruptions_interval

        if lam_per_sec < 0 or interruptions_interval <= 0:
            raise ValueError("Parâmetros inválidos: λ>=0 e Δt>0.")
        return float(1.0 - np.exp(-lam_per_sec * interruptions_interval))

    def setup(self):
 
        lam = self.estimate_lambda_from_csv()
        self.p = self.prob_from_lambda(lam)
        self.log(f"[INFO] Calibrado: lambda={lam:.6e}, p_interval={self.p:.6f} (Δt={self.interruptions_interval}s)")


    def sample_deletions(self, n_nodes) -> int:
        """
        Amostra K ~ Binomial(n_nodes, p): número de nós a deletar neste intervalo.
        """
        if n_nodes < 0:
            raise ValueError("n_nodes não pode ser negativo.")
        if not (0.0 <= self.p <= 1.0):
            raise ValueError("p deve estar em [0,1].")

        return int(self.rng.binomial(n=n_nodes, p=self.p))

    def emulation(self):

        while not (self.workload_event.is_set() and self.infrastructure_event.is_set()):
            self.log(f"[INFO] waiting {self.interruptions_interval}s until next interruption check...")
            time.sleep(self.interruptions_interval)

            nodes = self.k8s_api.list_node()

            # candidatos: exclui control-plane; trata labels None
            candidates = [
                n.metadata.name
                for n in nodes.items
                if "node-role.kubernetes.io/control-plane" not in (n.metadata.labels or {})
            ]

            n_nodes = len(candidates)
            self.log(f"[INFO] Current node count: {n_nodes}")

            if n_nodes <= 0:
                self.log("[INFO] No nodes available for deletion.")
                continue
            
            n_deletions = self.sample_deletions(n_nodes)

            self.log(f"[INFO] Sampling deletions: p={self.p:.6f}, nodes to delete={n_deletions}")
           
            if n_deletions > 0:
                size = min(n_deletions, n_nodes)
                nodes_to_delete = self.rng.choice(
                    candidates,
                    size=size,
                    replace=False
                )


                for node_name in nodes_to_delete:
                    self.k8s_api.delete_node(node_name)
                    self.log(f"[INFO] Deleted node: {node_name}")
            else:    
                self.log("[INFO] No nodes selected for deletion in this interval.")     
      
