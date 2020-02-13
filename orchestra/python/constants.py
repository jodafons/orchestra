

# ** Orchestra constants **

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



# ** Maestro constants **

#
# Flask
#
api_port = '5020'
CLUSTER_VOLUME = '/mnt/cluster-volume/'

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




