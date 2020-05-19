__all__ = ['Postman']

from orchestra import OrchestraDB
import sys
sys.path.append("/home/rancher/.cluster/orchestra/external")
from partitura.data import mail_credentials
from smtplib import SMTP
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader
import os

class Postman ():

  def __init__ (self):
    self.__myEmail = mail_credentials.login
    self.__myPassword = mail_credentials.pw
    self.__smtpServer = 'smtp.gmail.com'
    self.__smtpPort = 587
    self.__env = Environment(loader=FileSystemLoader('/home/rancher/.cluster/orchestra/orchestra/python/mailing/templates/'))
    self.__db = OrchestraDB()

  def __send (self, to_email, subject, bodyContent, logs=[]):
    # Building the e-mail
    message = MIMEMultipart()
    message['Subject'] = subject
    message['From'] = self.__myEmail
    message['To'] = to_email
    message.attach(MIMEText(bodyContent, 'html'))
    msgBody = message.as_string()
    for i, text in enumerate(logs):
      part = MIMEApplication(
        text,
        Name="LogFile_{}".format(i)
      )
      part['Content-Disposition'] = 'attachment; filename="%s"' % "LogFile_{}".format(i)
      msg.attach(part)
    # Authenticating
    server = SMTP(self.__smtpServer, self.__smtpPort)
    server.starttls()
    server.login(self.__myEmail, self.__myPassword)
    # Sending
    server.sendmail(self.__myEmail, to_email, msgBody.encode('utf-8'))
    # Quitting
    server.quit()

  def sendNotification (self, username, subject, message):
    user = self.__db.getUser(username)
    if user is None:
      return -1
    else:
      to_email = user.email
    template = self.__env.get_template('templates/task_notification.html')
    data = {}
    data['message'] = message
    output = template.render(data=data)
    self.__send(to_email, subject, output)

  def sendLogs (self, username, subject, message, logs=[]):
    user = self.__db.getUser(username)
    if user is None:
      return -1
    else:
      to_email = user.email
    template = self.__env.get_template('templates/task_notification.html')
    data = {}
    data['message'] = message
    output = template.render(data=data)
    self.__send(to_email, subject, output, logs)
