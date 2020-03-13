from orchestra.db.models import *

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from orchestra.enumerations import *



import hashlib
h = hashlib.md5()
h.update(b"rancher")
passwordHash = h.hexdigest()

engine = create_engine('postgres://postgres:postgres@localhost:5432/postgres')

Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

users = ["jodafons", "mverissi", "gabriel.milan"]
emails = ["jodafons@lps.ufrj.br", "micael.verissimo@lps.ufrj.br", "gabriel.milan@lps.ufrj.br"]


#for i in range(len(users)):
#  obj = Worker( username = users[i], maxPriority = 1000, passwordHash=passwordHash, email=emails[i] )
#  session.add(obj)



machines = ['node02','node03','node04','node05','node06','node07','node08','cessy','marselha','verdun']

for name in machines:
  obj = Node(name=name, CPUJobs=1, maxCPUJobs=30, GPUJobs=0, maxGPUJobs=0, queueName='lps', cluster=Cluster.LPS)
  session.add(obj)


session.commit()
session.close()

