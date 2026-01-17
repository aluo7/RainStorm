#!/usr/bin/env python3
import socket
import time
import sys

VM1_IP = "fa25-cs425-8501.cs.illinois.edu"
PORT = 9999
RATE = 20000   # lines per second

filename = sys.argv[1]  # exp1_input.csv or exp2_input.csv

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((VM1_IP, PORT))

with open(filename, "r") as f:
    for line in f:
        sock.sendall(line.encode("utf-8"))
        time.sleep(1.0 / RATE)

sock.close()
