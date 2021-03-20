# -*- coding: utf-8 -*-
"""

Created on Fri Mar 19 09:15:02 2021

This code requests people and planets data from SWAPI open API,
Creates two data frames (people and planets),
Connects to localhost MySQL database
Save the dataframes in the tables

HTTP status and error handling

@author: Javier Cebrián Casado
"""

import requests
import json
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import numpy as np

urlBase='https://swapi.dev/api/'
# URL of databases structure is:
# urlBase + database name + number:
# i.e: films atabase gives one json for each element:
#
#     https://swapi.dev/api/films/1/ to https://swapi.dev/api/films/6/
#
#     https://swapi.dev/api/films/ has a 'count' value with number of films.


def getData(jsonBase,database):
    """ 
    Gets dataframe from the database received as parameter
  
    Parameters: 
    jsonBase (dict): key = database name, value=url of this database
    database (str): key of database (i.e 'films','people','planets'...)
    
    Returns: 
    pd.DataFrame: dataframe of complete database
    """
    df = pd.DataFrame()
    # Request JSON with the number of urls in this database.
    rNumber = requests.get(jsonBase.get(database))
    rNumber.raise_for_status()     
    nUrl = rNumber.json().get('count') #number of urls to be downloaded
    for i in range(nUrl):
        #SWAPI returns 404 for character nº17
        if database == 'people' and i+1 == 17: 
            continue
        #Request each url ex: https://swapi.dev/api/people/70/
        rData = requests.get(urlBase+database+'/'+str(i+1)+'/')
        rData.raise_for_status()
        data = rData.json()
        df = df.append(data,ignore_index=True)
    if df.shape[0]! = nUrl:
        print(database+' database is not complete')
    return df


#CODE
try:
    # Request JSON with the url and name of all databases in SWAPI Web.
    r = requests.get(urlBase)
    r.raise_for_status()

    jsonBase = r.json()
    
    # printKeys(jsonBase) #Method to print available keys in the API (Debug)  

    #Get data of people
    peopleDf = pd.DataFrame()
    peopleDf = getData(jsonBase,'people').copy()
    
    #Get data of planets
    planetsDf = pd.DataFrame()
    planetsDf = getData(jsonBase,'planets').copy()
    
    peopleDf = peopleDf[['name','height','mass','hair_color','skin_color','eye_color',
                      'birth_year','gender','homeworld','url','edited']]
    
    #Some columns must be float, in addition like MySQL doesn't accept nan i use 0 as a unknown
    peopleDf['height'] = peopleDf['height'].apply(lambda x: 0 if x == 'unknown' else float(x.replace(',','.'))) 
    peopleDf['mass'] = peopleDf['mass'].apply(lambda x: 0 if x == 'unknown' else float(x.replace(',','.')))
    
    
    planetsDf = planetsDf[['name','rotation_period','orbital_period','diameter',
                        'climate','gravity','terrain','surface_water',
                        'population','url','edited']]
    
    #Some columns must be float, in addition like MySQL doesn't accept nan i use 0 as a unknown
    planetsDf['rotation_period'] = planetsDf['rotation_period'].apply(lambda x: 0 if x == 'unknown' else float(x.replace(',','.')))
    planetsDf['orbital_period'] = planetsDf['orbital_period'].apply(lambda x: 0 if x == 'unknown' else float(x.replace(',','.')))
    planetsDf['diameter'] = planetsDf['diameter'].apply(lambda x: 0 if x=='unknown' else float(x.replace(',','.')))
    planetsDf['surface_water'] = planetsDf['surface_water'].apply(lambda x: 0 if x == 'unknown' else float(x.replace(',','.')))
    planetsDf['population'] = planetsDf['population'].apply(lambda x: 0 if x == 'unknown' else float(x.replace(',','.')))


    #Connect with DB in localhost (create with MySQL WorkBench)
   
    sqlEngine = create_engine('mysql+pymysql://USER:PASSWORD@127.0.0.1/swapidb?charset=utf8')

    # dbConnection = sqlEngine.raw_connection()
    dbConnection = sqlEngine.connect()
    
    
    #Send to table pepople
    peopleDf.to_sql(name='people', con=dbConnection, if_exists='append',index=False,
                    dtype={ 
                   'name': sqlalchemy.types.VARCHAR(length=45),
                   'height': sqlalchemy.types.Float(precision=3, asdecimal=True),
                   'mass': sqlalchemy.types.Float(precision=3, asdecimal=True),
                   'hair_color': sqlalchemy.types.VARCHAR(length=45),
                   'skin_color': sqlalchemy.types.VARCHAR(length=45),
                   'eye_color': sqlalchemy.types.VARCHAR(length=45),
                   'birth_year': sqlalchemy.types.VARCHAR(length=45),
                   'gender': sqlalchemy.types.VARCHAR(length=45),
                   'homeworld': sqlalchemy.types.VARCHAR(length=45),
                   'url': sqlalchemy.types.VARCHAR(length=45),
                   'edited': sqlalchemy.DateTime()})
    
    
    planetsDf.to_sql(name='planets', con=dbConnection, if_exists='append',index=False,
                     dtype={ 
                   'name': sqlalchemy.types.VARCHAR(length=45),
                   'rotation_period': sqlalchemy.types.Float(precision=3, asdecimal=True),
                   'orbital_period': sqlalchemy.types.Float(precision=3, asdecimal=True),
                   'diameter': sqlalchemy.types.Float(precision=3, asdecimal=True),
                   'climate': sqlalchemy.types.VARCHAR(length=45),
                   'gravity': sqlalchemy.types.VARCHAR(length=45),
                   'terrain': sqlalchemy.types.VARCHAR(length=45),
                   'surface_water': sqlalchemy.types.Float(precision=3, asdecimal=True),
                   'population': sqlalchemy.types.Float(precision=3, asdecimal=True),                 
                   'url': sqlalchemy.types.VARCHAR(length=45),
                   'edited': sqlalchemy.DateTime()})
    
    dbConnection.close()
    
except (requests.HTTPError,json.JSONDecodeError,OSError) as exception:
        print(exception)

except mysql.connector.Error as err:
  print("Something went wrong with MySQL: {}".format(err))






#########################################################
#                   DEBUG METHODS                       #
#########################################################
def getUrlStatus(url):
    """ 
    Gets the status of a url.
  
    Parameters: 
    url (str): url to be checked
    
    Returns: 
    int: Status or -1 if OSError
    """
    try:
        r=requests.get(url)
        print('Status: '+str(r.status_code))
        return r.status_code
    except OSError:
        print("Connection aborted")
        return -1
#########################################################
def getReqStatus(req):
    """ 
    Gets the status of a request.
  
    Parameters: 
    req (obj): requests obj to be checked
    
    Returns: 
    int: Status or -1 if OSError
    """
    try:
        print('Status: '+str(req.status_code))
        return req.status_code
    except OSError:
        print("Connection aborted")
        return -1
#########################################################
def printKeys(dictionary):
    """
    Prints key values from a DICT
  
    Parameters: 
    dictionary (dict): dictionary to be printed
    
    """
    print('Dictionary Contains:')
    for k in dictionary.keys():
        print(str(k))
#########################################################
def getNumberReq(jsonBase,database):
    """
    DEBUG method to get the number of urls
  
    jsonBase (dict): key=database name, value=url of this database
    database (str): key of database (i.e 'films','people','planets'...)
    
    Returns: 
    count (int): number of urls
    
    """
    urlData=jsonBase.get(database)
    rData=requests.get(urlData)
        
    rData.raise_for_status()
    return rData.json().get('count')
#########################################################
#☺Alternative connectors
# cnx = mysql.connector.connect(user='USER', password='XXXXXXXX',
                                  # host='127.0.0.1',
                                  # database='swapidb')
# sqlEngine = create_engine('mysql+mysqlconnector://USER:PASSWORD@127.0.0.1/swapidb?charset=utf8')
#Send to table planets
# cnx.close()
