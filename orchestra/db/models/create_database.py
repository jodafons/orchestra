
from orchestra import getConfig
from orchestra.db.models import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

config = getConfig()


engine = create_engine(config["postgres"])

Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)



#worker = Worker(
#            username='jodafons',
#            email='jodafons@lps.ufrj.br',
#            volume='/Users/jodafons/Desktop/orchestra/output',
#	    )
#
#session.add(worker)
#session.commit()
#
#
#
#nvidia_machines    = ['verdun']
#cpu_small_machines = ['verdun']
#
#
#for name in nvidia_machines:
#  obj = Node(queueName='nvidia', name=name, jobs=0, maxJobs=0)
#  session.add(obj)
#
#for name in cpu_small_machines:
#  obj = Node(queueName='cpu_small', name=name, jobs=0, maxJobs=2)
#  session.add(obj)


session.commit()
session.close()


