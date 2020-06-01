import os



# The cluster templates
LOGFILE_NAME = "job_configId_%d.log"
OUTPUT_DIR = "job_configId_%d"
CLUSTER_VOLUME = '/mnt/cluster-volume' # LPS default



# queues
allow_queue_names = ["cpu_small" , "cpu_large", "nvidia"]


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






