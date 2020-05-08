import os



# The cluster job template to used

CLUSTER_JOB_TEMPLATE = '../data/job_template.yaml'
CLUSTER_RANCHER_CREDENTIALS = os.environ["CLUSTER_RANCHER_CREDENTIALS"]
CLUSTER_POSTGRES_URL = os.environ["CLUSTER_POSTGRES_URL"]
CLUSTER_VOLUME = '/mnt/cluster-volume' # LPS default






# ** Orchestra constants **

# Others
MINUTE = 60
HOUR = 60*MINUTE
MAX_TEST_JOBS=1
MIN_SUCCESS_JOBS =1
MAX_FAILED_JOBS = 1

# Time to update all priorities from the schedule
MAX_UPDATE_TIME = 1 * MINUTE

# Max trial from kubernetes
MAX_FAIL=1

# For DB configuration
NUMBER_OF_TRIALS=3;




# ** Maestro constants **

#
# Flask
#

#API_PORT = '5020'
API_PORT = os.environ["CLUSTER_API_PORT"]

#
# Error codes
#
ERR_OK				= 200
ERR_UNAUTHORIZED 	= 401
ERR_NOT_FOUND       = 404

#
#	Hashing
#
salt = b'g\xc2U\x1c\x9fx\xddxm\xf1\x85\x85w\x8dA\xa3`\x07F\xfdHP\x87Zc\xe8\xea\xcc\xcd\xd7fz'
iterations = 100000





#
# Others
#

CLUSTER_VOLUME_SDUMONT = '/'




