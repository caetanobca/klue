"""
DeploymentsGenerator is a utility class for generating Kubernetes deployment objects
based on input data from a pandas DataFrame. It provides methods to create, delete,
and scale deployments, as well as to format CPU and memory resource values.
Converts the CPU value to millicores (m) and returns it as a string.
"""
import pandas as pd

class DeploymentsGenerator:
    def __init__(self, karpenter=True, cluster_autoscaler=False):
        self.karpenter = karpenter
        self.cluster_autoscaler = cluster_autoscaler

    def put_cpu_unity(self, value):
        """
        Converts the CPU value to millicores (m) and returns it as a string.
        """
        return f"{int(float(value) * 1000)}m"

    def put_memory_unity(self, value):
        """
        Converts the memory value from bytes to MiB and returns it as a string.
        """
        mebibytes = int(value) // (2 ** 20)
        return f"{mebibytes}Mi"
    
    def topology_spread_rule(self, spec, labels):
        required = spec.get("required", True)
        max_skew = spec.get("max_skew", 1)

        if required:
            rule_type = "DoNotSchedule"
        else:
            rule_type = "ScheduleAnyway"

        topology_spread = [
            {
                "maxSkew": max_skew,
                "whenUnsatisfiable": rule_type,
                "labelSelector": {"matchLabels": labels},
                "topologyKey": "kubernetes.io/hostname",
            }
        ]
        
        return topology_spread

    def anti_affinity_rule(self, spec, labels):
        required = spec.get("required", True)
        namespace = spec.get("namespace")

        if required:
            rule_type = "requiredDuringSchedulingIgnoredDuringExecution"
            affinity = {
                "podAntiAffinity": {
                    rule_type: [
                        {
                            "labelSelector": {"matchLabels": labels},
                            "namespaces": [namespace],
                            "topologyKey": "kubernetes.io/hostname"
                        }
                    ]
                }
            }
        else:
            rule_type = "preferredDuringSchedulingIgnoredDuringExecution"
            affinity = {
                "podAntiAffinity": {
                    rule_type: [
                        {
                            "weight": 1,
                            "podAffinityTerm": {
                                "labelSelector": {"matchLabels": labels},
                                "namespaces": [namespace],
                                "topologyKey": "kubernetes.io/hostname"
                            }
                        }
                    ]
                }
            }

        return affinity

    def reliability_rule(self, spec, labels):
        """
        Configura a regra de reliability retornando as configurações necessárias.
        Retorna um dicionário com 'annotations' e 'schedulerName'.
        """
        min_availability = spec.get("min-availability", 1)
        
        return {
            "annotations": {
                "reliability.scheduler/min-availability": str(min_availability)
            },
            "schedulerName": "reliability-scheduler"
        }

    def _determine_scheduler_name(self, rule_type, reliability_config=None):
        """
        Determina qual scheduler usar baseado no tipo de regra e configuração.
        Para reliability, usa o schedulerName retornado pela regra.
        """
        if rule_type == "reliability" and reliability_config:
            return reliability_config.get("schedulerName")
        elif not self.cluster_autoscaler:
            return "custom-scheduler"
        else:
            return None

    def _build_pod_metadata(self, labels, rule_type, reliability_config=None):
        """
        Constrói o metadata do pod incluindo labels e annotations.
        Para reliability, usa as annotations retornadas pela regra.
        """
        metadata = {"labels": labels}
        
        if rule_type == "reliability" and reliability_config:
            annotations = reliability_config.get("annotations", {})
            if annotations:
                metadata["annotations"] = annotations
        
        return metadata

    def _build_affinity(self, row, extra_affinity=None):
        """
        Constrói a configuração de affinity baseada no tipo de autoscaler.
        """
        affinity = {}
        
        if self.karpenter:
            affinity = {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {
                                "matchExpressions": [
                                    {
                                        "key": "karpenter.sh/nodepool",
                                        "operator": "In",
                                        "values": [row["nodepool"]],
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        elif self.cluster_autoscaler:
            affinity = {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {
                                "matchExpressions": [
                                    {
                                        "key": "node-role.kubernetes.io/control-plane",
                                        "operator": "DoesNotExist"
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        
        # Merge extra affinity (como anti-affinity rules) mantendo nodeAffinity
        if extra_affinity:
            for key, value in extra_affinity.items():
                if key == "podAntiAffinity":
                    affinity[key] = value
                # nodeAffinity já foi definido acima, não sobrescrever
        
        return affinity

    def _build_tolerations(self, row):
        """
        Constrói a lista de tolerations baseada no tipo de autoscaler.
        """
        if self.cluster_autoscaler:
            return [
                {
                    "key": "kwok-provider",
                    "operator": "Equal",
                    "value": "true",
                    "effect": "NoSchedule"
                }
            ]
        else:
            toleration_key = "kwok.x-k8s.io/node" if not self.karpenter else row['nodepool']
            return [
                {
                    "key": toleration_key,
                    "operator": "Exists",
                    "effect": "NoSchedule"
                }
            ]

    def generate_applied_deployments(self, group: pd.DataFrame, rule_spec=None):
        """
        Generates a dictionary of applied deployments based on the rows of the DataFrame.
        """
        # Determinar tipo de regra
        rule_type = rule_spec.get("rule_type") if rule_spec else "no_rule"
        
        # Mapear tipo de regra para função e nome
        rule_mapping = {
            "topology_spread": (self.topology_spread_rule, "topologySpreadConstraints"),
            "anti_affinity": (self.anti_affinity_rule, "affinity"),
            "reliability": (self.reliability_rule, "reliability"),
            "no_rule": (None, None)
        }
        
        if rule_type not in rule_mapping:
            raise ValueError(f"Unknown rule: {rule_type}")
        
        get_rule, rule_name = rule_mapping[rule_type]

        applied_deployments = {}

        for _, row in group[group['action'] == 'create'].iterrows():
            if row["owner_kind"].lower() not in ['deployment', 'statefulset']:
                continue

            labels = {
                "app": row['replicaset'],
                "deployment": row['replicaset']
            }

            # Processar regras específicas
            extra_spec = {}
            extra_affinity = None
            reliability_config = None
            
            if rule_name and get_rule:
                if rule_type == "anti_affinity":
                    deploy_rule_spec = rule_spec.copy()
                    deploy_rule_spec['namespace'] = str(row["namespace"])
                
                rule_result = get_rule(deploy_rule_spec, labels)
                
                if rule_name == "affinity":
                    extra_affinity = rule_result
                elif rule_name == "reliability":
                    reliability_config = rule_result
                else:
                    extra_spec[rule_name] = rule_result

            # Construir as partes do pod spec
            tolerations = self._build_tolerations(row)
            scheduler_name = self._determine_scheduler_name(rule_type, reliability_config)
            pod_metadata = self._build_pod_metadata(labels, rule_type, reliability_config)

            # Construir affinity completa
            affinity = self._build_affinity(row, extra_affinity)

            # Montar pod template spec
            pod_spec = {
                "affinity": affinity,
                "tolerations": tolerations,
                "containers": [
                    {
                        "name": "fake-container",
                        "image": "fake-image",
                        "resources": {
                            "requests": {}
                        },
                    }
                ],
                **extra_spec
            }

            # Adicionar schedulerName se necessário
            if scheduler_name:
                pod_spec["schedulerName"] = scheduler_name

            # Adicionar recursos de CPU e memória
            if row['cpu'] != 'NA':
                pod_spec["containers"][0]["resources"]["requests"]["cpu"] = self.put_cpu_unity(row['cpu'])
            if row['memory'] != 'NA':
                pod_spec["containers"][0]["resources"]["requests"]["memory"] = self.put_memory_unity(row['memory'])

            # Montar pod template completo
            pod_template = {
                "metadata": pod_metadata,
                "spec": pod_spec
            }

            # Criar deployment
            deployment = {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": row['replicaset'], "namespace": row['namespace']},
                "spec": {
                    "replicas": row['pods'],
                    "selector": {"matchLabels": labels},
                    "template": pod_template,
                },
            }

            # Adicionar ao dicionário de deployments
            if row['namespace'] not in applied_deployments:
                applied_deployments[row['namespace']] = [deployment]
            else:
                applied_deployments[row['namespace']].append(deployment)

        return applied_deployments

    def generate_deleted_deployments(self, group: pd.DataFrame):
        """
        Generates a list of deployments to be deleted based on the rows of the DataFrame.
        """
        deleted_deployments = []

        for _, row in group[group['action'] == 'delete'].iterrows():
            deleted_deployments.append({"name": row['replicaset'], "namespace": row['namespace']})

        return deleted_deployments

    def generate_scaled_deployments(self, group: pd.DataFrame):
        """
        Generates a list of scaled replicasets based on the rows of the DataFrame.
        """
        scaled_deployments = []

        for _, row in group[group['action'] == 'scale'].iterrows():
            scaled_deployments.append({
                "name": row['replicaset'], 
                "namespace": row['namespace'], 
                "pods": row['pods'], 
                "kind": row['owner_kind'].lower()
            })

        return scaled_deployments