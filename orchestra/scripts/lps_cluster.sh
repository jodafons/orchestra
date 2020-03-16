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
screen -d -m -S API_MAESTRO    bash -c 'sleep 60 && cd /home/rancher/devs/orchestra && source setup.sh && cd orchestra/scripts && source update_api.sh && python3 run_api.py 2>&1 | tee /home/rancher/OUTPUT_ApiMaestro.txt'

# Orchestra schedule
screen -d -m -S MAIN_ORCHESTRA bash -c 'sleep 60 && cd /home/rancher/devs/orchestra && source setup.sh && cd orchestra/scripts && python3 run_lps_rancher.py 2>&1 | tee /home/rancher/OUTPUT_OrchestraSchedule.txt'

# Web apps
screen -d -m -S WEB_DASHBOARD  bash -c 'sleep 60 && cd /home/rancher/devs/orchestra && source setup.sh && cd orchestra/python/dashboard && python3 main.py 2>&1 | tee /home/rancher/OUTPUT_WebDashboard.txt'
