#!/bin/bash

USER="sameern3"
SSH_KEY="~/.ssh/cs425-vm-key"
HOSTS=( \
    "fa25-cs425-8501.cs.illinois.edu" \
    "fa25-cs425-8502.cs.illinois.edu" \
    "fa25-cs425-8503.cs.illinois.edu" \
    "fa25-cs425-8504.cs.illinois.edu" \
    "fa25-cs425-8505.cs.illinois.edu" \
    "fa25-cs425-8506.cs.illinois.edu" \
    "fa25-cs425-8507.cs.illinois.edu" \
    "fa25-cs425-8508.cs.illinois.edu" \
    "fa25-cs425-8509.cs.illinois.edu" \
    "fa25-cs425-8510.cs.illinois.edu" \
)

# Directory on the VMs where run_experiment.sh writes output
REMOTE_OUTPUT_DIR="~/spark_job_outputs"

# Local directory to save all outputs (doesn't work)
LOCAL_OUTPUT_DIR="./experiment_output"
mkdir -p "$LOCAL_OUTPUT_DIR"

for i in "${!HOSTS[@]}"; do
    HOST="${HOSTS[$i]}"
    echo "========== Collecting output from VM $(($i + 1)): $HOST =========="

    # Create a subfolder per VM
    mkdir -p "$LOCAL_OUTPUT_DIR/$HOST"

    # Copy all files from remote output directory
    scp -i "$SSH_KEY" -r "$USER@$HOST:$REMOTE_OUTPUT_DIR/*" "$LOCAL_OUTPUT_DIR/$HOST/"

    echo "Output from $HOST saved to $LOCAL_OUTPUT_DIR/$HOST"
done

echo "All VM outputs have been collected locally."
