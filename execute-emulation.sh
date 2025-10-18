#!/bin/bash

# Função para exibir a ajuda
usage() {
    echo "Uso: $0 [--dev | --sim] [--use-cluster CONTEXT] [--data-path PATH] [--nodepool-path PATH] [--use-karpenter] [--skip-tracer] [--static-infra] [--static-workload] [-h | --help]"
    echo ""
    echo "Opções:"
    echo "  --emulation-name NAME                     Nome da emulação"
    echo "  --dev                                     Criar ambiente de desenvolvimento"
    echo "  --sim                                     Criar ambiente de emulação"
    echo "  --use-cluster CONTEXT                     Usar um cluster existente (passe o nome do contexto)"
    echo "  --data-path PATH                          Especificar o caminho do trace a ser usado na emulação"
    echo "  --nodepool-path PATH                      Especificar o caminho do nodepool a ser usado na emulação"
    echo "  --use-karpenter                           Ativar o Karpenter para gerenciamento de nós"
    echo "  --cluster-autoscaler-provider-template    Especificar o caminho do template do cluster autoscaler"
    echo "  --use-kubernetes-cluster-autoscaler       Ativar o Kubernetes Autoscaler para gerenciamento de nós"
    echo "  --skip-tracer                             Pular a execução do tracer"
    echo "  --static-infra                            Usar infraestrutura estática na emulação"
    echo "  --static-workload                         Usar workload estático na emulação"
    echo "  --allocation-rule PATH                    Especificar o caminho da regra de alocação a ser usada (Opcional)"
    echo "  --speed-up FACTOR                         Define um fator de aceleração para a emulação (ex: 2, 5, 10)."
    echo "                                            Observação: ao utilizar este parâmetro, certifique-se de que o novo intervalo entre eventos seja superior a 30 segundos."
    echo "  --skip-pods-mapping                       Pular o mapeamento de pods e a execução do custom-scheduler"
    echo "  --use-interruption-model                  Ativar o modelo de interrupção de instâncias spot"
    echo "  --node-interruption-rate 0-1              Especificar a taxa de interrupção de instâncias spot"
    echo "  --node-interruption-interval SECONDS      Define o intervalo de interrupção de instâncias spot em segundos (Padrão: 300)"
    echo "  --interruption-random-seed SEED           Define a semente para a geração de números aleatórios para interrupções (Padrão: 42)"
    echo "  -h, --help                                Exibir esta mensagem de ajuda"
    exit 1
}

# Função para configurar um cluster existente
use_existing_cluster() {
    local context=$1
    echo "Usando o cluster existente: $context"
    kubectl config use-context "$context"
}

# Parsing das flags
ENVIRONMENT=""
CLUSTER_ACTION=""
CLUSTER_CONTEXT=""
TRACE_PATH=""
NODEPOOL_PATH=""
TRACER="no-skip"
KARPENTER="karpenter-off"
INFRASTRUCTURE="dynamic"
WORKLOAD="dynamic"
EMULATION_NAME=""
ALLOCATION_RULE=""
SPEED_UP_FACTOR=""
SKIP_PODS_MAPPING=""
INTERRUPTION_ENABLED="no"
NODE_INTERRUPTION_RATE=""
NODE_INTERRUPTION_INTERVAL=300
INTERRUPTION_RANDOM_SEED=42

while [[ $# -gt 0 ]]; do
    case $1 in
        --emulation-name)
            EMULATION_NAME="$2"
            shift 2
            ;;
        --dev)
            ENVIRONMENT="development"
            shift
            ;;
        --sim)
            ENVIRONMENT="emulation"
            shift
            ;;
        --use-cluster)
            CLUSTER_ACTION="use"
            CLUSTER_CONTEXT="$2"
            shift 2
            ;;
        --data-path)
            TRACE_PATH="$2"
            shift 2
            ;;
        --nodepool-path)
            NODEPOOL_PATH="$2"
            shift 2
            ;;
        --use-karpenter)
            KARPENTER="karpenter-on"
            shift
            ;;
        --cluster-autoscaler-provider-template)
            CLUSTER_AUTOSCALER_PROVIDER_TEMPLATE="$2"
            shift 2
            ;;
        --use-kubernetes-cluster-autoscaler)
            KUBERNETES_AUTOSCALER="kubernetes-autoscaler-on"
            shift
            ;;
        --skip-tracer)
            TRACER="skip-tracer"
            shift
            ;;
        --static-infra)
            INFRASTRUCTURE="static"
            shift
            ;;
        --static-workload)
            WORKLOAD="static"
            shift
            ;;
        --allocation-rule)
            ALLOCATION_RULE="$2"
            shift 2
            ;;
        --speed-up)
            SPEED_UP_FACTOR="$2"
            shift 2
            ;;
        --skip-pods-mapping)
            SKIP_PODS_MAPPING="skip-pods-mapping"
            shift
            ;;
        --use-interruption-model)
            INTERRUPTION_ENABLED="yes"
            shift
            ;;
        --node-interruption-rate)
            NODE_INTERRUPTION_RATE="$2"
            shift 2
            ;;
        --node-interruption-interval)
            NODE_INTERRUPTION_INTERVAL="$2"
            shift 2
            ;;
        --interruption-random-seed)
            INTERRUPTION_RANDOM_SEED="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Opção inválida: $1"
            usage
            ;;
    esac
done

# Validação das flags
if [[ -z $ENVIRONMENT ]]; then
    echo "Erro: É necessário especificar --dev ou --sim."
    usage
fi

if [[ $CLUSTER_ACTION == "use" && -z $CLUSTER_CONTEXT ]]; then
    echo "Erro: O nome do contexto é necessário ao usar --use-cluster."
    usage
fi

if [[ $ENVIRONMENT == "emulation" && -z $TRACE_PATH ]]; then
    echo "Erro: É necessário especificar --data-path ao usar --sim."
    usage
fi

if [[ $ENVIRONMENT == "emulation" && $KARPENTER == "karpenter-on" && -z $NODEPOOL_PATH ]]; then
    echo "Erro: É necessário especificar --nodepool-path ao usar --sim com --use-karpenter."
    usage
fi

if [[ $ENVIRONMENT == "emulation" && $KUBERNETES_AUTOSCALER == "kubernetes-autoscaler-on" && -z $CLUSTER_AUTOSCALER_PROVIDER_TEMPLATE ]]; then
    echo "Erro: É necessário especificar --cluster-autoscaler-provider-template ao usar --sim com --use-kubernetes-autoscaler."
    usage
fi

if [[ $KUBERNETES_AUTOSCALER == "kubernetes-autoscaler-on" && $KARPENTER == "karpenter-on" ]]; then
    echo "Erro: Não é possível usar Karpenter e Kubernetes Cluster Autoscaler ao mesmo tempo."
    usage
fi

# Configuração do ambiente
if [[ $ENVIRONMENT == "development" ]]; then
    echo "Configurando ambiente de desenvolvimento..."
    cd kwok-karpenter-install
    ./setup.sh "$KARPENTER"
elif [[ $ENVIRONMENT == "emulation" ]]; then
    echo "Configurando ambiente de emulação..."
    cd kwok-karpenter-install
    ./setup.sh "$KARPENTER" "$KUBERNETES_AUTOSCALER" "$CLUSTER_AUTOSCALER_PROVIDER_TEMPLATE"
    cd ..
    
    # Construir comando Python com argumentos opcionais
    PYTHON_CMD="python3 src/main.py \"$TRACE_PATH\" \"$INFRASTRUCTURE\" \"$WORKLOAD\""
    
    # Adicionar argumentos opcionais se especificados
    if [[ -n $NODEPOOL_PATH ]]; then
        PYTHON_CMD="$PYTHON_CMD --nodepool-path \"$NODEPOOL_PATH\""
    fi
    
    if [[ $KARPENTER == "karpenter-on" ]]; then
        PYTHON_CMD="$PYTHON_CMD --karpenter"
    fi
    
    if [[ $KUBERNETES_AUTOSCALER == "kubernetes-autoscaler-on" ]]; then
        PYTHON_CMD="$PYTHON_CMD --cluster-autoscaler"
    fi
    
    if [[ $TRACER == "skip-tracer" ]]; then
        PYTHON_CMD="$PYTHON_CMD --skip-tracer"
    fi
    
    if [[ $SKIP_PODS_MAPPING == "skip-pods-mapping" ]]; then
        PYTHON_CMD="$PYTHON_CMD --skip-pods-mapping"
    fi

    if [[ -n $EMULATION_NAME ]]; then
        PYTHON_CMD="$PYTHON_CMD --emulation-name \"$EMULATION_NAME\""
    fi
    
    if [[ -n $ALLOCATION_RULE ]]; then
        PYTHON_CMD="$PYTHON_CMD --allocation-rule \"$ALLOCATION_RULE\""
    fi
    
    if [[ -n $SPEED_UP_FACTOR ]]; then
        PYTHON_CMD="$PYTHON_CMD --speed-up \"$SPEED_UP_FACTOR\""
    fi

    if [[ $INTERRUPTION_ENABLED == "yes" ]]; then
        PYTHON_CMD="$PYTHON_CMD --use-interruption-model"
        
        if [[ -n $NODE_INTERRUPTION_RATE ]]; then
            PYTHON_CMD="$PYTHON_CMD --interruption-rate \"$NODE_INTERRUPTION_RATE\""
        fi
        
        PYTHON_CMD="$PYTHON_CMD --node-interruption-interval \"$NODE_INTERRUPTION_INTERVAL\""
        PYTHON_CMD="$PYTHON_CMD --interruption-random-seed \"$INTERRUPTION_RANDOM_SEED\""
    fi

    # Executar comando Python
    echo "Executando: $PYTHON_CMD"
    eval $PYTHON_CMD
fi