#!/bin/bash

USER="sameern3"
SSH_KEY="~/.ssh/cs425-vm-key"
VM1="fa25-cs425-8501.cs.illinois.edu"

echo "Stopping experiment components on VM1..."

ssh -i "$SSH_KEY" "$USER@$VM1" " \
    pkill -f 'nc -lk 9999' || true; \
    pkill -f 'spark_stream_exp' || true; \
    pkill -f 'send_stream.py' || true; \
"

echo "All components stopped."
