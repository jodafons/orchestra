


# Others
MINUTE = 60
HOUR = 60*MINUTE


MAX_TEST_JOBS=1
MIN_SUCCESS_JOBS =1
MAX_FAILED_JOBS = 1

# Time to update all priorities from the schedule
MAX_UPDATE_TIME = 20 #5*MINUTE


# Max trial from kubernetes
MAX_FAIL=1

# For DB configuration
NUMBER_OF_TRIALS=3;

# LPS storage and database hosted in zeus machine
DEFAULT_URL_LPS='postgres://postgres:postgres@localhost:5432/postgres'
BASEPATH_SG_LPS = '/mnt/cluster-volume'


# Let's concetatre all user in the joao account. Here, we will like an storage for all sub-users.
#DEFAULT_URL_SDUMONT='postgres://postgres:postgres@postgres.cahhufxxnnnr.us-east-2.rds.amazonaws.com:5432/postgres'
#DEFAULT_URL_SDUMONT='postgres://sdumont:postgres@sdumont.cahhufxxnnnr.us-east-2.rds.amazonaws.com:5432/sdumont'
DEFAULT_URL_SDUMONT='postgres://sdumont:postgres@sdumont.cahhufxxnnnr.us-east-2.rds.amazonaws.com:5432/postgres'
#BASEPATH_SG_SDUMONT = '/prj/atlas-ringerid/joao.pinto'
BASEPATH_SG_SDUMONT = '/volume'



