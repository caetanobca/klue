from kubernetes import client, watch
from util.k8s_api.k8s_api import K8SAPI

class KubernetesObjectApplier:
    def __init__(self, k8s_api: K8SAPI):
        """
        Inicializa a classe com a API do Kubernetes e uma função de log.
        """
        self.k8s_api = k8s_api
    
    def log(self, message):
        """
        Logs a message with a "[KUBERNETES APPLIER]" prefix.
        """
        print(f"[KUBERNETES APPLIER] {message}")

    def wait_deployment_ready(self, name: str, namespace: str):
        apps_v1 = client.AppsV1Api()
        
        # Checa estado atual antes de abrir o watch
        deployment = apps_v1.read_namespaced_deployment(name=name, namespace=namespace)
        spec_replicas = deployment.spec.replicas or 1
        if (deployment.status.ready_replicas or 0) >= spec_replicas:
            return
    
        w = watch.Watch()
        for event in w.stream(
            apps_v1.list_namespaced_deployment,
            namespace=namespace,
            field_selector=f"metadata.name={name}"
        ):
            deployment = event["object"]
            spec_replicas = deployment.spec.replicas or 1
            if (deployment.status.ready_replicas or 0) >= spec_replicas:
                w.stop()
                return

    def apply_deployment(self, obj, namespace, name):
        """
        Aplica um objeto do tipo Deployment ao cluster.
        """
        try:
            self.k8s_api.read_namespaced_deployment(name, namespace)
            self.k8s_api.patch_namespaced_deployment(name, namespace, obj)
            self.log(f"[INFO] Deployment {name} updated in namespace {namespace}.")
        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.k8s_api.create_namespaced_deployment(namespace, obj)
                self.log(f"[INFO] Deployment {name} created in namespace {namespace}.")

    def apply_nodeclaim(self, obj, name):
        """
        Aplica um objeto do tipo NodeClaim ao cluster.
        """
        try:
            obj["metadata"].pop("resourceVersion", None)
            self.k8s_api.patch_infrastructure_object(obj, "karpenter.sh", "v1", "nodeclaims", name)
            self.log(f"[INFO] Nodeclaim {name} updated")
        except client.exceptions.ApiException as e:
            if e.status == 404:
                obj["metadata"].pop("resourceVersion", None)
                self.k8s_api.create_infrastructure_object(obj, "karpenter.sh", "v1", "nodeclaims")
                self.log(f"[INFO] Nodeclaim {name} created")

    def apply_node(self, obj, name):
        """
        Aplica um objeto do tipo Node ao cluster.
        """
        try:
            obj["metadata"].pop("resourceVersion", None)
            self.k8s_api.patch_infrastructure_object(obj)
            self.log(f"[INFO] Node {name} updated")
        except client.exceptions.ApiException as e:
            if e.status == 404:
                obj["metadata"].pop("resourceVersion", None)
                self.k8s_api.create_infrastructure_object(obj)
                self.log(f"[INFO] Node {name} created")

    def apply_object(self, obj):
        """
        Aplica um objeto Kubernetes ao cluster, delegando para a função apropriada.
        """
        kind = obj.get("kind", "").lower()
        namespace = obj["metadata"].get("namespace", "default")
        name = obj["metadata"]["name"]

        if kind == "deployment":
            self.apply_deployment(obj, namespace, name)
        elif kind == "nodeclaim":
            self.apply_nodeclaim(obj, name)
        elif kind == "node":
            self.apply_node(obj, name)
        else:
            self.log(f"[ERROR] Unsupported kind: {kind}")
