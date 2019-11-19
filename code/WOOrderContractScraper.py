import camelot
import pandas as pd
import numpy as np
import pdfminer as pm
import math
from datetime import datetime  
from datetime import timedelta  
import warnings
import os
import subprocess
import ocrmypdf
import sys
import time
import timeout_decorator
import traceback
from utils import parseTimeRange, fixHeaders, findHeaderRow

@timeout_decorator.timeout(300, use_signals=False)
def processOrder(tables, committee, station, reportformat):
    ## Create the ad day/times df.
    ads = pd.DataFrame(columns=['Start Date', 'End Date', 'Weekdays', 'Spots/Week', 'Rate', 'Rating', 'index'])
    ## Create the shows DF.
    shows = pd.DataFrame(columns=['index', 'Title'])
    secondlines = pd.DataFrame(columns=['index', 'Title'])
    ## Add second line of inventory code
    def mergeSLines(row):
        if row.name+1 < len(shows):
            nextRow = shows.iloc[row.name+1]
            sline = secondlines[(row['index'] < secondlines['index']) & (nextRow['index']>secondlines['index'])]
        else: 
            sline = secondlines[(row['index'] < secondlines['index'])]
        if(len(sline)>0):
            if(row['Title'] != sline['Title'].values[0]):
                row['Title'] = sline['Title'].values[0]
        else:
            pass
            #print('Second line merge error at ad: ', row.name)
        return row
    
    def getSecondLines(df):
        returnVal = pd.DataFrame(columns=['index', 'Title'])
        #Get show lines
        progs = df[(df['Amount'] != '') & (df['Ch'] != '')]
        progs = progs.iloc[1:] # Remove residual header row.
        slines = list(map(lambda x: x+1, progs.index.values))
        if not (slines[-1] < len(df)): ## In case the second show line is on the next page.
            #print('Program second line on next page.')
            slines = slines[:-1]
        slines = df.iloc[slines]
        slines = slines.replace('', np.nan, regex=True)
        slines = slines[slines.isnull().sum(axis=1) >= len(slines.columns)-2]
        for index, row in slines.iterrows():
            nulls = pd.isnull(row)
            nulls = nulls[nulls!=True]
            returnVal = returnVal.append({'index': index, 'Title': row[nulls.index[0]]}, ignore_index=True)
        returnVal['index'] = returnVal['index'] + lastIndex
        return returnVal
    '''
    def addNameIfDiff(p):
        for val in nlines.iloc[p.name]:
            if(p[progColName] != val) & (val.isna() != True):
                p[progColName] = p[progColName] + val
        return p
    '''
    ## Merge program names into ad days.
    def mergeProgs(i):
        p = shows[shows['index']<=i]
        if len(p)>0:
            p = p.iloc[len(p)-1]
            return p['Title']
        else:
            return ""
    ## Create individual records for each ad spot.  
    ## Filter out day times rows.                
    def getDays(row):
        #print(row)
        nextRow = 1
        dayCols = ['Start Date', 'End Date', 'Weekdays', 'Spots/Week', 'Rate']
        for val in row:
            try:
                ind = dayCols.index(val)
                dayCols.remove(val)
            except:
                pass
        if(len(dayCols) == 0):
            if(len(headers)==0):
                headers.append(buys.loc[(row.name), :])
            while (nextRow!=-1):
                dayColFound=False
                if((row.name+nextRow) != len(buys)):
                    potRow = buys.loc[(row.name+nextRow), :]
                    valCount=0
                    for k in potRow:
                        if 'Week:' in k:
                            if k.split(':')[1]:
                                fixVal = k.split(':')[1]
                                potRow[valCount+1] = fixVal
                            nextRow=nextRow+1
                            dayColFound=True
                        valCount = valCount+1
                    if dayColFound != True:
                        nextRow=-1
                    else:
                        headers.append(potRow)
                else:
                    nextRow=-1
    ## Filter out program names.
    lastIndex = 0
    for t in tables:
        progColName=False
        headerConfig = t.apply(findHeaderRow, axis=1).dropna()
        if len(headerConfig)>0:
            progColName = headerConfig.values[0][0]
        if progColName != False:
            ## Figure out inventory code lines
            progs = t.copy()
            if progColName == 'Description':
                progs = progs.apply(fixHeaders, axis=1)
                progs.columns = progs.iloc[headerConfig.values[0][1]]
            else:
                progs.columns = progs.iloc[headerConfig.values[0][1]]
            progs = progs.iloc[headerConfig.values[0][1]:]
            progs = progs.reset_index(drop=True)
            ## Some WideOrbit reports have two line program names, this gets the second lines.
            if progColName == 'Inventory Code':
                slines = getSecondLines(progs)
                secondlines = secondlines.append(slines)
            ## Get the program name rows
            progs = progs[(progs['Amount'] != '') & (progs['Ch'] != '')] 
            progs = progs.iloc[1:] # Remove residual header row.
            if progColName == 'Description':
                progs['Description'] = progs['Description'] + progs['Desc2']
            progs = progs.reset_index()
            ## Make index run continously accross pages.
            progs['index'] = progs['index'] + lastIndex
            lastIndex = t.iloc[len(t)-1].name+lastIndex+1
            ## Remove duplicate columns and append to the master list.
            progs = progs.loc[:,~progs.columns.duplicated()]
            progs = progs.rename(columns={progColName:'Title'})
            progs = progs[['index', 'Title']]
            shows = shows.append(progs, sort=False)
        else:
            pass
            #print("False")
    ## Merge in the second lines to complete the program names.
    if progColName == 'Inventory Code':
        shows = shows.reset_index(drop=True)
        shows = shows.apply(mergeSLines, axis=1)
    ## Seperate out the days and times each ad will be on.
    lastIndex = 0
    for t in tables:
        progColName=False
        headerConfig = t.apply(findHeaderRow, axis=1).dropna()
        if len(headerConfig)>0:
            progColName = headerConfig.values[0][0]
        if progColName != False:
            headers = []
            buys = t.copy()
            buys = buys.dropna(how='all')
            buys = buys.iloc[headerConfig.values[0][1]:]
            buys = buys.reset_index(drop=True)
            buys.apply(getDays, axis=1)
            days = pd.DataFrame(headers)
            if len(days)>0:
                days = days.replace('', np.nan)
                days = days.dropna(axis='columns')
                days.columns = days.iloc[0]
                days = days[1:]
                days = days.reset_index()
                days['index'] = days['index'] + lastIndex
                lastIndex = t.iloc[len(t)-1].name+lastIndex+1
                ads = ads.append(days, sort=False)
    ## Add in Program Names
    ads['Program'] = ads['index'].apply(mergeProgs)
    ads['Time'] = None
    ads = ads.drop(columns=['index'], axis=1)
    ads = ads.drop_duplicates()
    ads = ads.apply(parseTimeRange, filetype=reportformat, axis=1)
    '''
    print('SHOWS')
    print(shows)
    print('ADS')
    print(ads[['index', 'Weekdays']])
    print(len(adtimes))
    '''
    ## Expand each ad buy listing to individual spots
    return ads