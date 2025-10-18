#/bin/bash

DATA_PATH=$1
CLUSTER_AUTOSCALER_PROVIDER_TEMPLATE=$2
EMULATION_NAME=$3
SPEED_UP_FACTOR=$4
SPOT_LIFETIME_PATH=$5
NODE_INTERRUPTION_INTERVAL=$6
INTERRUPTION_RANDOM_SEED=$7
RULE_PATH=$8


./create-cluster.sh 3

./execute-emulation.sh \
    --emulation-name "$EMULATION_NAME" \
    --sim \
    --data-path "$DATA_PATH" \
    --cluster-autoscaler-provider-template "$CLUSTER_AUTOSCALER_PROVIDER_TEMPLATE" \
    --use-kubernetes-cluster-autoscaler \
    --static-infra \
    --skip-pods-mapping \
    --speed-up "$SPEED_UP_FACTOR" \
    --use-interruption-model \
    --node-interruption-rate "$SPOT_LIFETIME_PATH" \
    --node-interruption-interval "$NODE_INTERRUPTION_INTERVAL" \
    --interruption-random-seed "$INTERRUPTION_RANDOM_SEED" \
    --allocation-rule "$RULE_PATH" 

./delete-cluster.sh 3

rm /tmp/*.csv
rm /tmp/*.json
