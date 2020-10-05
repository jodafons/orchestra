export LC_ALL=''
export RCM_NO_COLOR=0
export RCM_GRID_ENV=0

# Set all envs
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH


# Set all orchestra configurations
export ORCHESTRA_PATH=`pwd`
export ORCHESTRA_POSTGRES_URL="postgres://ringer:12345678@ringer.cef2wazkyxso.us-east-1.rds.amazonaws.com:5432/postgres"
export ORCHESTRA_JOB_COMPLETE_FILE=".job_complete"




