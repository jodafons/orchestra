# Imports
from app import app, db
from time import sleep
from config import INITIAL_SLEEP_TIME

class WebApp ():

  def run (self):
    print ("Running WebApp...")
    app.run(host="0.0.0.0", port=8080, debug=False)

web = WebApp()

if __name__ == '__main__':
  web.run()
