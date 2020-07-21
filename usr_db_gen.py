# User DataBase Generator

import random
from datetime import datetime
from datetime import timedelta  
import sys
import os
sys.path.append(os.path.abspath('..'))
sys.path.append(os.path.abspath('bot'))

from influxdb import InfluxDBClient

"""
Definition of paramater that allow connecting to the SocialDistance DB
"""

from config_shared import INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_DBUSER, INFLUXDB_DBPASSWORD, INFLUXDB_DBNAME, INFLUXDB_USER, INFLUXDB_PASSWORD
from config_shared import TABELA_MV, TABELA_TOTAL, TABELA_TRACE

import json
import time

USER_LIST  =  [
    "maralves@cisco.com", 
    "lpavanel@cisco.com", 
    "aluciade@cisco.com", 
    "acassemi@cisco.com", 
    "dvicenti@cisco.com",
    "flcorrea@cisco.com"
]

# DB Functions

"""TimeSeries DataBase Class."""

# Objeto do banco
# metodo de escrita do trace

class DBClient():
  """
  This class allows to instantiate a handler for the SocialDistance InfluxDB database.
  When a object is instantitated, connection to the database is opened.
  """
  def __init__(self):

    # Connects to the database
    self._host = INFLUXDB_HOST
    self._port = INFLUXDB_PORT
    self._user = INFLUXDB_USER
    self._password = INFLUXDB_PASSWORD
    self._dbname = INFLUXDB_DBNAME      
    self._client = InfluxDBClient(self._host, self._port, self._user, self._password, self._dbname)
    
  def Close(self):
    self._client.close()

  def peopleLog(self, local: str, userid: str, status: str, origem: str,time: str):
      """
      Escreve no banco quando user entra ou sai
      """
     
      # Prepare JSON with data to be writte in PeopleCount measurement
      json_body = {}
      json_body["measurement"] = TABELA_TRACE
      json_body["time"] = time
      json_body["tags"] = {}
      json_body["tags"]["local"] = local
      json_body["fields"] = {}
      json_body["fields"]["userid"] = userid
      json_body["fields"]["state"]=status
      json_body["fields"]["origin"]=origem

      # Write data to InfluxDB
      self._client.write_points([json_body])

      #Return True to indicate that data was recorded
      return True

  def TotalCount(self, local: str, total: int, origem: str, people:list):
      """
      Escreve no banco total de usuarios naquele local
      """
     
      # Prepare JSON with data to be writte in PeopleCount measurement
      json_body = {}
      json_body["measurement"] = TABELA_TOTAL
      json_body["time"] = time
      json_body["tags"] = {}
      json_body["tags"]["local"] = local
      json_body["tags"]["origin"] = origem
      json_body["tags"]["people"] = people
      json_body["fields"] = {}
      json_body["fields"]["count"] = total
      
      # Write data to InfluxDB
      self._client.write_points([json_body])

      #Return True to indicate that data was recorded
      return True


BANCO=DBClient()


def UserGenerator (initial_date:str, days:int, frequency:int, room:str, user_max:int):

    #Everthing starts with the first day:

    lastest_meeting = 20
    users_id = []

    #db_date = datetime.strptime(initial_date, "%d-%m-%y") + timedelta(hours=1)  
    db_date = datetime.strptime(initial_date, "%d-%m-%y")

    for day in range(days):
        hour = 9
        db_date = db_date + timedelta(hours = hour)
        db_date_str = db_date.strftime('%Y%m%dT%H')
        print ("\n")
        while hour < lastest_meeting:
            users_right_now = random.randint(0, user_max)
            users_id = random.sample(USER_LIST, users_right_now)
            print (str(db_date) + ": In Day " + str(day) + \
                " at " + str(hour) + \
                "h, the number of people in the room " + room + \
                " is: " + str(users_right_now))
            #print ("The users are: " + str(users_id))
            
            for user in users_id:
                print("banco.peopleLog(" + room + "," + str(user) +  ",entrou,wifi," + str(db_date_str))
                BANCO.peopleLog(room,str(user),"entrou","wifi", str(db_date_str))


            hour += frequency
            db_date = db_date + timedelta(hours = frequency)
            db_date_str = db_date.strftime('%Y%m%dT%H')

            for user in users_id:
                print("banco.peopleLog(" + room + "," + str(user) +  ",saiu,wifi," + str(db_date_str))
                BANCO.peopleLog(room,str(user),"saiu","saiu", str(db_date_str))

        
        if (hour == lastest_meeting):
            db_date = db_date + timedelta(hours=4)



def main():
    print("This application feeds the DB with synthetic data.")
    print("-------------------------------------------------")
    print("Here is the info needed: \n\n")

    initial_date = str(input("Starting date(dd-mm-yy): "))
    days = int(input("Number of days: "))
    frequency = int(input("What's the average frequency (in hours): "))
    room = str(input("Room name: "))
    user_max = int(input("Number of users at the same time (max of 5): "))

    #UserGenerator (initial_date = "20-07-20", days = 3, frequency = 1, room = "SALA DE TESTES", user = 5)

    print ("\n\n-------------------------------------------------")
    print ("This generator will create a synthetic data starting on " + initial_date + \
        " using work-hours from 9-19h for " + str(days) + " days long." + \
        " The users will enter and exit the room using a frequency of " + str(frequency) + \
        " hour(s) of the room " + room + " with a max number of " + str(user_max) + " users ")
    print ("-------------------------------------------------\n\n")
    
    UserGenerator (initial_date, days, frequency, room, user_max)

    BANCO.Close()

    #print ("Have a nice one!")

if __name__ == '__main__':
    main()