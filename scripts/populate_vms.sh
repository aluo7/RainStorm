#!/bin/bash

USER="alanluo3"
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

DIRS_TO_COPY=("src" "scripts") # copies both mp1/... and mp3/... dirs
DEST_PATH="~"
DEMO_DATA="demo_data/business"

SERVER_LOG_0="server.log"
SERVER_LOG_1="server.fd.log"
SERVER_LOG_2="server.hydfs.log"
SERVER_LOG_3="err_server.log"
DGREP_SERVER_LOG="dgrep_server.log"

for i in "${!HOSTS[@]}"; do
    (
        HOST="${HOSTS[$i]}"
        echo "========== Configuring VM $(($i + 1)): $HOST =========="

        echo "- Cleaning and creating directories on $HOST..."
        ssh -i "$SSH_KEY" "$USER@$HOST" "rm -rf mp1 mp3 mp4 $DEMO_DATA hydfs_storage* *.log *.txt $SERVER_LOG_0 $SERVER_LOG_1 $SERVER_LOG_2 $SERVER_LOG_3 $DGREP_SERVER_LOG && mkdir -p $DEST_PATH/mp1 $DEST_PATH/mp3 $DEST_PATH/mp4 $DEST_PATH/demo_data"

        # Copying files
        echo "- Copying files to $HOST..."
        cd mp1/
        scp -q -r -i "$SSH_KEY" "${DIRS_TO_COPY[@]}" "$USER@$HOST:$DEST_PATH/mp1"
        cd ../mp3/
        scp -q -r -i "$SSH_KEY" "${DIRS_TO_COPY[@]}" "$USER@$HOST:$DEST_PATH/mp3"
        cd ../mp4/
        scp -q -r -i "$SSH_KEY" "${DIRS_TO_COPY[@]}" "$USER@$HOST:$DEST_PATH/mp4"
        # scp -q -r -i "$SSH_KEY" "$DEMO_DATA" "$USER@$HOST:$DEMO_DATA"
        cd ..

        echo "- Building binaries on $HOST..."
        ssh -i "$SSH_KEY" "$USER@$HOST" "go build -o $DEST_PATH/mp1/dgrep_server $DEST_PATH/mp1/src/dgrep_server.go && \
                                        go build -o $DEST_PATH/mp1/dgrep_client $DEST_PATH/mp1/src/dgrep_client.go && \
                                        go build -o $DEST_PATH/mp3/server $DEST_PATH/mp3/src/server.go $DEST_PATH/mp3/src/membership_list.go $DEST_PATH/mp3/src/hydfs.go $DEST_PATH/mp3/src/failure_detection.go && \
                                        go build -o $DEST_PATH/mp4/server $DEST_PATH/mp4/src/server.go $DEST_PATH/mp4/src/membership_list.go $DEST_PATH/mp4/src/hydfs.go $DEST_PATH/mp4/src/failure_detection.go $DEST_PATH/mp4/src/rainstorm.go"

        echo "========== Finished VM $(($i + 1)): $HOST =========="
    ) &
done

echo "Waiting for all VMs to finish configuring..."
wait
echo "All VMs configured."
