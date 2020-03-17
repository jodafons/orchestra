#!/bin/bash

### BEGIN INIT INFO
# Provides:          lps_cluster.sh
# Required-Start:    $network $syslog
# Required-Stop:     $network $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start lps_cluster.sh at boot time
# Description:       Enable service provided by lps_cluster.sh.
### END INIT INFO

#
# Runs everything
#

# APIs
screen -d -m -S API_MAESTRO    bash -c 'sleep 60 && cd /home/rancher/.cluster/orchestra && git pull && source setup.sh && cd external/partitura/scripts && python3 run_maestro.py 2>&1 | tee /home/rancher/.cluster/logs/OUTPUT_ApiMaestro.txt'

# Orchestra schedule
screen -d -m -S MAIN_ORCHESTRA bash -c 'sleep 60 && cd /home/rancher/.cluster/orchestra && source setup.sh && cd external/partitura/scripts && python3 run_orchestra.py 2>&1 | tee /home/rancher/.cluster/logs/OUTPUT_OrchestraSchedule.txt'

# Web apps
screen -d -m -S WEB_DASHBOARD  bash -c 'sleep 60 && cd /home/rancher/.cluster/orchestra && source setup.sh && cd orchestra/python/dashboard && python3 main.py 2>&1 | tee /home/rancher/.cluster/logs/OUTPUT_WebDashboard.txt'
