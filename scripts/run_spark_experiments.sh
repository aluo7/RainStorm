#!/bin/bash

USER="sameern3"
SSH_KEY="~/.ssh/cs425-vm-key"

# VM hostnames (VM1 = leader)
HOSTS=(
    "fa25-cs425-8501.cs.illinois.edu"  # vm1 / leader
    "fa25-cs425-8502.cs.illinois.edu"
    "fa25-cs425-8503.cs.illinois.edu"
    "fa25-cs425-8504.cs.illinois.edu"
    "fa25-cs425-8505.cs.illinois.edu"
    "fa25-cs425-8506.cs.illinois.edu"
    "fa25-cs425-8507.cs.illinois.edu"
    "fa25-cs425-8508.cs.illinois.edu"
    "fa25-cs425-8509.cs.illinois.edu"
    "fa25-cs425-8510.cs.illinois.edu"
)

VM1="${HOSTS[0]}"
LEADER_URL="spark://${VM1}:7077"

JOBS_DIR="~/spark_jobs"
LOCAL_JOBS_DIR="./data"

LOG_DIR="~/spark_logs"

K=$1

if [[ "$K" != "1" && "$K" != "2" && "$K" != "3" && "$K" != "4" ]]; then
    echo "Invalid experiment number: $K"
    exit 1
fi

echo "=============== Running Experiment $K ==============="


if [ "$K" -eq 1 ]; then
    SPARK_SCRIPT="exp1.py"
    CSV_FILE="exp1_input.csv"
elif [ "$K" -eq 2 ]; then
    SPARK_SCRIPT="exp2.py"
    CSV_FILE="exp2_input.csv"
elif [ "$K" -eq 3 ]; then
    SPARK_SCRIPT="exp3.py"
    CSV_FILE="exp3_input.csv"
elif [ "$K" -eq 4 ]; then
    SPARK_SCRIPT="exp4.py"
    CSV_FILE="exp4_input.csv"
fi

# Copy files to VM
echo "[0/4] Copying experiment files to VM1 ($VM1)..."

ssh -i "$SSH_KEY" "$USER@$VM1" "mkdir -p $JOBS_DIR"

FILES_TO_COPY=(
    "$SPARK_SCRIPT"
    "send_stream.py"
    "$CSV_FILE"
)

for FILE in "${FILES_TO_COPY[@]}"; do
    if [ ! -f "$LOCAL_JOBS_DIR/$FILE" ]; then
        echo "ERROR: Missing local file: $LOCAL_JOBS_DIR/$FILE"
        exit 1
    fi

    scp -i "$SSH_KEY" "$LOCAL_JOBS_DIR/$FILE" "$USER@$VM1:$JOBS_DIR/"
done

echo "Files copied to VM1."

# Start TCP server
# echo "[1/4] Starting TCP server on VM1 ($VM1:9999)..."

# ssh -i "$SSH_KEY" "$USER@$VM1" " \
#     pkill -f 'nc -lk 9999' 2>/dev/null || true; \
#     mkdir -p $LOG_DIR; \
#     nohup nc -lk 9999 > $LOG_DIR/nc_server.log 2>&1 &
# "

# echo "TCP server started."

# Make Spark job
echo "[2/4] Starting Spark job on VM1..."

ssh -i "$SSH_KEY" "$USER@$VM1" " \
    source ~/.bashrc; \
    cd $JOBS_DIR; \
    nohup spark-submit \
        --master $LEADER_URL \
        --deploy-mode client \
        $SPARK_SCRIPT > $LOG_DIR/spark_exp${K}.log 2>&1 &
"

echo "Spark experiment $K started."

# Start input stream
echo "[3/4] Starting CSV sender from VM1..."

ssh -i "$SSH_KEY" "$USER@$VM1" " \
    source ~/.bashrc; \
    cd $JOBS_DIR; \
    nohup python3 send_stream.py $CSV_FILE > $LOG_DIR/sender_exp${K}.log 2>&1 &
"

echo "Sender running. Data is streaming."

# Print final
echo "===================================================="
echo "Experiment $K launched!"
echo "  Files copied from:     $LOCAL_JOBS_DIR/"
echo "  Spark Leader UI:       http://${VM1}:8080"
echo "  Logs:"
echo "    $VM1:$LOG_DIR/spark_exp${K}.log"
echo "    $VM1:$LOG_DIR/sender_exp${K}.log"
echo "    $VM1:$LOG_DIR/nc_server.log"
echo "===================================================="
echo "To stop everything: ./stop_experiment.sh"
echo "===================================================="
