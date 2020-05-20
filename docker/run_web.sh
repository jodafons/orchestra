#!/bin/bash
cd /home/rancher/.cluster/orchestra && git pull && source setup.sh && cd orchestra/python/dashboard && python3 main.py 2>&1 | tee /home/rancher/.cluster/logs/OUTPUT_WebDashboard.txt
