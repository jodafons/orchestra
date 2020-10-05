__all__ = ['Postman']

from Gaugi import Logger
from Gaugi.messenger.macros import *

from smtplib import SMTP
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader

import os, sys

class Postman (Logger):

  def __init__ (self, email, password,  templates):
    Logger.__init__(self)
    self.__myEmail = email
    self.__myPassword = password
    self.__smtpServer = 'smtp.gmail.com'
    self.__smtpPort = 587
    self.__env = Environment(loader=FileSystemLoader(templates))


  def __send (self, to_email, subject, bodyContent, logs=[]):

    try:
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
        message.attach(part)
      # Authenticating
      server = SMTP(self.__smtpServer, self.__smtpPort)
      server.starttls()
      server.login(self.__myEmail, self.__myPassword)
      # Sending
      server.sendmail(self.__myEmail, to_email, msgBody.encode('utf-8'))
      # Quitting
      server.quit()
    except Exception as e:
      MSG_WARNING(self, e)


  def send (self, to_email, subject, message):
    template = self.__env.get_template('templates/task_notification.html')
    data = {}
    data['message'] = message
    output = template.render(data=data)
    self.__send(to_email, subject, output)


