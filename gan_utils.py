import dataiku as dk 
import datetime as dt
import requests as rq
import pandas as pd
import numpy as np
import json
from multiprocessing import  Pool

from bs4 import BeautifulSoup as Soup

def merge_two_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return(z)

def get_schema(lst):
    """ Build a standard dataiku.Dataset schema. """
    
    def prepare_schema(el):
        if type(el)!= tuple:
            return(el,"string")
        else:
            return(el)
        
    lstSch = [prepare_schema(el) for el in lst]
    return([{"name":l[0],"type":l[1]} for l in lstSch])

def assess_type(test):
    if not test or test!=test :
        return('NULL')
    try :
        int(test)
        return('INT')
    except ValueError:
        pass
    try:
        float(test)
        return('FLOAT')
    except ValueError:
        pass
    
    return('TEXT')

def get_soup(link,headerName=None,params=None,verify=None):
    """ The deaders have to be defined as a custom variables at the project level. """
    if headerName: 
        headers = json.loads(dk.get_custom_variables()[headerName])
    else:
        headers = None 
        
    if not verify :
        r = rq.get(link,headers=headers,params=params)
    else:
        r = rq.get(link,headers=headers,params=params,verify=verify)
    soup = Soup(r.text,'html.parser')
    return(soup)

def pooling(fct,lst,nb_pool=8):
    
    """Open different pools for the same function. Return the output of each pool in a list"""
    p = Pool(nb_pool)
    infos = p.map(fct,lst)
    p.terminate()
    p.join()
    return infos

def date_index_df(df,colDate):
    # Should be added to general.utils function
    df[colDate] = pd.to_datetime(df[colDate].values)
    df.index = df[colDate]
    return df

def get_showered(dirtyPig):
    """Clean a dirty string"""
    return str(dirtyPig.encode('utf-8')).translate(None,"\r\n\t").strip(' ').decode('utf-8')

def string_to_list_dict(dirtyPig):
    """Transform a string representing a list of dict into an actual list of dicts"""
    # Remove '[{'   '}]' at the beginning and the end of the string
    dirtyPig = dirtyPig[2:-2]
    # Splitting on '},{' - making a str_dict by addinge '{' and '}' - loading as a dict
    cleanLst = [json.loads("{"+el+"}") for el in dirtyPig.split('},{')]
    return cleanLst
