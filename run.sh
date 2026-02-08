#!/bin/bash

DATA_PATH=$1
CLUSTER_AUTOSCALER_PROVIDER_TEMPLATE=$2
EMULATION_NAME=$3
SPEED_UP_FACTOR=$4
SPOT_LIFETIME_PATH=$5
NODE_INTERRUPTION_INTERVAL=$6
INTERRUPTION_RANDOM_SEED=$7
RELIABILITY_SCHEDULER=$8
RULE_PATH=$9


./create-cluster.sh 3

# 1. Base Command (tudo exceto os argumentos condicionais/finais)
EMULATION_CMD="./execute-emulation.sh \
    --emulation-name \"$EMULATION_NAME\" \
    --sim \
    --data-path \"$DATA_PATH\" \
    --cluster-autoscaler-provider-template \"$CLUSTER_AUTOSCALER_PROVIDER_TEMPLATE\" \
    --use-kubernetes-cluster-autoscaler \
    --static-infra \
    --skip-pods-mapping \
    --speed-up \"$SPEED_UP_FACTOR\" \
    --use-interruption-model \
    --node-interruption-rate \"$SPOT_LIFETIME_PATH\" \
    --node-interruption-interval \"$NODE_INTERRUPTION_INTERVAL\" \
    --interruption-random-seed \"$INTERRUPTION_RANDOM_SEED\""

# 2. Adiciona o argumento condicional (--reliability-scheduler)
if [ "$RELIABILITY_SCHEDULER" = "reliability-scheduler-on" ]; then
    EMULATION_CMD="$EMULATION_CMD --reliability-scheduler"
fi

# 3. Adiciona o argumento final (--allocation-rule) com aspas duplas corretas
# Este será sempre o último argumento adicionado.
EMULATION_CMD="$EMULATION_CMD --allocation-rule \"$RULE_PATH\""

echo "$EMULATION_CMD"
eval "$EMULATION_CMD"

#./delete-cluster.sh 3

rm /tmp/*.csv
rm /tmp/*.json
