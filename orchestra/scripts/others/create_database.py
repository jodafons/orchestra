from orchestra.db.models import *

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.enumerations import *
from Gaugi import load


users = load('users.pic.gz')

engine = create_engine('postgres://postgres:postgres@localhost:5432/lps_prod')

Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)





#for user in users:
#
#  #roles = [ Role(name='user') ]
#  #if user['isAdmin']:
#  #  roles.append( Role(name='superuser') )
#  #for role in roles:
#  #  session.add(role)
#  #session.commit()
#
#  worker = Worker(
#              username=user['username'],
#              email=user['email'],
#              password=user['password'],
#              allowedQueues=user['allowedQueues'],
#              maxPriority=user['maxPriority'],
#              #roles=roles,
#               )
#
#  session.add(worker)
#  session.commit()







nvidia_machines    = ['node04','node05','cessy','marselha','verdun']
cpu_small_machines = ['node02','node03','node04','node05','node06','cessy','marselha','verdun']
cpu_large_machines = ['node07','node08']


for name in nvidia_machines:
  obj = Node(queueName='nvidia', name=name, jobs=0, maxJobs=0, cluster=Cluster.LPS)
  session.add(obj)

for name in cpu_small_machines:
  obj = Node(queueName='cpu_small', name=name, jobs=0, maxJobs=10, cluster=Cluster.LPS)
  session.add(obj)

for name in cpu_large_machines:
  obj = Node(queueName='cpu_large', name=name, jobs=0, maxJobs=1, cluster=Cluster.LPS)
  session.add(obj)




session.commit()
session.close()

