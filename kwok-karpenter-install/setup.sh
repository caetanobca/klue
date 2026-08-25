go install github.com/google/ko@latest
export PATH=$PATH:~/go/bin
source ~/.bashrc

if [ $# -lt 1 ]; then
	echo "Uso: $0 <karpenter-on/karpenter-off>"
	exit 1
fi

KARPENTER="$1"
KUBERNETES_AUTOSCALER="$2"
CLUSTER_AUTOSCALER_PROVIDER_TEMPLATE="$3"
RELIABILITY_SCHEDULER="$4"

CA_EXPANDER="${CA_EXPANDER:-least-waste}"
echo "CA_EXPANDER: $CA_EXPANDER"
# Setup Prometheus and Grafana

kubectl create namespace monitoring

kubectl apply --server-side -f kube-prometheus/manifests/setup
kubectl wait \
	--for condition=Established \
	--all CustomResourceDefinition \
	--namespace=monitoring
kubectl apply -f kube-prometheus/manifests/
kubectl patch prometheus k8s -n monitoring --type merge -p '{"spec":{"retention":"15d"}}'
kubectl patch deployment kube-state-metrics -n monitoring --type merge -p '{"spec":{"template":{"spec":{"containers":[{"name":"kube-state-metrics","resources":{"limits":{"cpu":"500m","memory":"2Gi"},"requests":{"cpu":"50m","memory":"512Mi"}}}]}}}}'

docker login

if [ "$KARPENTER" = "karpenter-on" ]; then
	cd karpenter-code

	make toolchain
	make build
	make install-kwok
	make apply
	make gen_instance_types

	cd ..

	kubectl apply -f configuration-files/karpenter-servicemonitor.yml

	echo "Instalando o KWOK"
	./install-kwok.sh

elif [ "$KUBERNETES_AUTOSCALER" = "kubernetes-autoscaler-on" ]; then
	if [ -z "$CLUSTER_AUTOSCALER_PROVIDER_TEMPLATE" ]; then
		echo "Erro: É necessário especificar --cluster-autoscaler-provider-template ao usar --use-kubernetes-autoscaler."
		exit 1
	fi

	echo "Instalando o KWOK"
	./install-kwok.sh

	# Always deploy the scheduler (TimingPlugin runs in all scenarios)
	kubectl create namespace kube-scheduler-reliability --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f reliability-scheduller/rbac.yaml
	kubectl apply -f reliability-scheduller/configmap-env.yaml -n kube-scheduler-reliability

	if [ "$RELIABILITY_SCHEDULER" = "reliability-scheduler-on" ]; then
		echo "Configurando com o Reliability Scheduler"

		kubectl apply -f reliability-scheduller/prometheus-rules.yaml
		kubectl apply -f reliability-scheduller/configmap.yaml          # with RS + TimingPlugin
		kubectl apply -f reliability-scheduller/deployment.yaml

	else
		echo "Configurando sem o Reliability Scheduler (apenas TimingPlugin)"

		kubectl apply -f reliability-scheduller/configmap-timing-only.yaml  # without RS, with TimingPlugin
		kubectl apply -f reliability-scheduller/deployment.yaml
	fi

	# Create 300 KWOK nodes upfront (32vcpu-512gb — largest capacity in template)
	# Eliminates CA scheduling interference and isolates scheduler performance
	echo "Criando 500 nós KWOK (32vcpu-512gb)..."
	NODES_YAML=$(mktemp)
	for i in $(seq -w 1 300); do
		cat >> "$NODES_YAML" <<EOF
---
apiVersion: v1
kind: Node
metadata:
  name: kwok-node-$i
  annotations:
    kwok.x-k8s.io/node: fake
  labels:
    type: kwok
    kwok-nodegroup: 64vcpu-512gb-group
	kubernetes.io/hostname: kwok-node-$i
status:
  capacity:
    cpu: "64"
    ephemeral-storage: 1019013632Ki
    memory: 512Gi
    pods: "110"
  allocatable:
    cpu: "64"
    ephemeral-storage: 1019013632Ki
    memory: 512Gi
    pods: "110"
  conditions:
  - lastHeartbeatTime: "2023-05-31T04:39:58Z"
    lastTransitionTime: "2023-05-31T04:39:46Z"
    type: Ready
    status: "True"
    reason: KubeletReady
    message: "fake node ready"
EOF
	done
	kubectl apply -f "$NODES_YAML"
	rm "$NODES_YAML"
	echo "300 nós criados."


	kubectl apply -f reliability-scheduller/configmap-env.yaml -n kube-scheduler-reliability

	while [[ $(kubectl get pod prometheus-k8s-0 -n monitoring -o jsonpath='{.status.phase}') != "Running" ]]; do
		sleep 5
	done

	# kubectl taint nodes klue-cluster-control-plane node-role.kubernetes.io/control-plane=:NoSchedule
else
	echo "Karpenter e Kubernetes Cluster Autoscaler estão desativados."
	echo "Instalando o KWOK"
	./install-kwok.sh
fi



while [[ $(kubectl get pod prometheus-k8s-0 -n monitoring -o jsonpath='{.status.phase}') != "Running" ]]; do
  sleep 5
done

kubectl patch networkpolicy prometheus-k8s -n monitoring --type=json -p '[{"op":"add","path":"/spec/ingress/-","value":{"ports":[{"port":9090,"protocol":"TCP"}]}}]'
