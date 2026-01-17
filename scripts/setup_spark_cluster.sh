#!/bin/bash


USER="sameern3"
SSH_KEY="~/.ssh/cs425-vm-key"

# VM1 is the leader
HOSTS=(
    "fa25-cs425-8501.cs.illinois.edu"  # vm1 (leader)
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

SPARK_VERSION="4.0.1"
SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
SPARK_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"
INSTALL_DIR="~/spark"

# Download and install Spark
for i in "${!HOSTS[@]}"; do
(
    HOST="${HOSTS[$i]}"
    echo "========== Setting up VM $(($i+1)): $HOST =========="

    ssh -i "$SSH_KEY" "$USER@$HOST" " \
        echo '- Cleaning old Spark...' && \
        rm -rf spark ${SPARK_TGZ} && \
        rm -rf spark_logs/ && \
        rm -rf spark_jobs/ && \
        rm -rf spark_job_outputs/ && \
        echo '- Downloading Spark...' && \
        wget -q ${SPARK_URL} && \
        echo '- Extracting Spark...' && \
        tar -xzf ${SPARK_TGZ} && \
        mv spark-${SPARK_VERSION}-bin-hadoop3 spark && \
        echo '- Updating .bashrc...' && \
        grep -qxF 'export SPARK_HOME=\$HOME/spark' ~/.bashrc || echo 'export SPARK_HOME=\$HOME/spark' >> ~/.bashrc && \
        grep -qxF 'export PATH=\$SPARK_HOME/bin:\$PATH' ~/.bashrc || echo 'export PATH=\$SPARK_HOME/bin:\$PATH' >> ~/.bashrc && \
        python -m ensurepip --upgrade && \
        python -m pip install pyspark && \
        mkdir spark_logs && \
        mkdir spark_job_outputs \

    "

    echo "========== Finished VM $(($i+1)): $HOST =========="
) &
done

echo "Waiting for all VMs to finish Spark install..."
wait
echo "All VMs finished Spark installation."
echo ""

# Start Spark leader
LEADER_HOST="${HOSTS[0]}"
echo "========== Starting Spark Leader on $LEADER_HOST =========="

ssh -i "$SSH_KEY" "$USER@$LEADER_HOST" " \
    source ~/.bashrc; \
    \$SPARK_HOME/sbin/stop-master.sh >/dev/null 2>&1 || true; \
    \$SPARK_HOME/sbin/start-master.sh; \
"

echo "Spark Leader started on $LEADER_HOST"
echo ""

# Start Spark workers
LEADER_URL="spark://${LEADER_HOST}:7077"

for i in "${!HOSTS[@]}"; do
    if [ $i -eq 0 ]; then continue; fi  # skip leader

    HOST="${HOSTS[$i]}"
    echo "========== Starting Spark Worker on $HOST =========="

    ssh -i "$SSH_KEY" "$USER@$HOST" " \
        source ~/.bashrc; \
        \$SPARK_HOME/sbin/stop-worker.sh >/dev/null 2>&1 || true; \
        \$SPARK_HOME/sbin/start-worker.sh ${LEADER_URL}; \
    " &
done

echo "Waiting for all workers to start..."
wait

echo ""
echo "============================================="
echo "Spark Cluster is LIVE:"
echo "  Leader: ${LEADER_URL}"
echo "  Web UI: http://${LEADER_HOST}:8080"
echo "============================================="
