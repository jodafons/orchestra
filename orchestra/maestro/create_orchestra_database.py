

from orchestra.db.models import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


engine = create_engine('postgres://ringer:12345678@ringer.cef2wazkyxso.us-east-1.rds.amazonaws.com:5432/postgres')
Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)
session.commit()
session.close()


