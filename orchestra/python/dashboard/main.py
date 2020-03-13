# Imports
from app import app, validate_database, build_initial_db, db
from time import sleep
from config import INITIAL_SLEEP_TIME

class WebApp ():
  
  def run (self):
    if (not validate_database()):
      print ("Building the initial database...")
      build_initial_db()
    print ("Running WebApp...")
    app.run(host="0.0.0.0", port=8080, debug=False)

web = WebApp()

if __name__ == '__main__':
  print ("Will wait for {} seconds for start...".format(INITIAL_SLEEP_TIME))
  sleep (INITIAL_SLEEP_TIME)
  print ("Will start now!!!")
  web.run()