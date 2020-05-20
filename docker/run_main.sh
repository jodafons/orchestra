#!/bin/bash
cd /home/rancher/.cluster/orchestra && git pull && source setup.sh && cd orchestra/scripts && python3 run_orchestra.py 2>&1 | tee /home/rancher/.cluster/logs/OUTPUT_OrchestraSchedule.txt
