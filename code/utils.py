import camelot
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
import traceback
import re
from fuzzywuzzy import fuzz
import timeout_decorator


## Read in all table regions

@timeout_decorator.timeout(500, use_signals=False)
def convertPDFToTable(filepath, cols):
    table_cols = [cols for y in range(10)]
    p1t = camelot.read_pdf(filepath, 
                         strip_text='.\n', flavor='stream', pages="1-end",
                         #table_regions=['0,700,600,0'],
                          columns=table_cols, 
                           edge_tol=1000)
    pdf = []
    for t in p1t:
        pdf.append(t.df)
    return pdf

# Generates keys for predicting what form a PDF is in.
def createWOFileTypeKeys():
    keys = {}
    for ftype in list(os.scandir('/media/andrew/F08C9B848C9B444E/analysis/tv/orderscoring/')):
        o = list(os.scandir('/media/andrew/F08C9B848C9B444E/analysis/tv/orderscoring/'+ftype.name+'/'))
        op = []
        # Convert all pdfs to text, string process them and turn them into an array of strings
        for f in o:
            res = subprocess.run(['pdftotext', f.path, '-'], 
                                                stdout=subprocess.PIPE).stdout.decode()
            res = res.split('\n')
            res = [x.replace(' ', '') for x in res ]
            res = [x.replace(':', '') for x in res ]
            res = [x.replace('.', '') for x in res ]
            res = list(filter(lambda a: a != '', res))
            res = res[0:200]
            res = list(set(res))
            #print(res[0:100])
            if len(res)>1:
                op.append(res)
            else:
                ocrmypdf.ocr(f.path, f.path, deskew=True, rotate_pages=True)
        curSet = []
        # Filter so only keys that exist in all files of the given report format remain.
        print(len(op))
        for l in op:
            if len(curSet)==0:
                curSet=l
            else:
                curSet = [x for x in curSet if x in l]
        keys[ftype.name] = curSet
    # Filter out keys that are non-unique to that report type.
    for k in keys:
        types = ['contracts', 'invoices', 'orders']
        types = [x for x in types if x != k]
        for t in types:
            keys[k] = [x for x in keys[k] if x not in keys[t]]
        # Get rid of nonspecific keys
        keys[k] = list(filter(lambda a: (len(a)>4) & (len(a)<23) , keys[k]))
        print(keys[k])
    keys = [[(k, vv) for vv in v] for k, v in keys.items()]
    keys2 = []
    for k in keys:
        for v in k:
            keys2.append(v)
    keys = pd.DataFrame(keys2, columns=['pdftype', 'keyword'])
    keys.to_csv('filetypekeywords.csv', index=False)

def pacFromPath(p): # This needs to be expanded to deal with erroneous subfolders.
    p = p.split('/')
    station = p[(p.index('Political Files')-1)]
    pac = p[len(p)-1]
    return {'station': station, 'pac': pac}
def parseTimeRange(d, filetype): ## ONLY PARSE OUT IF CONTRACT
    rs = [m.start() for m in re.finditer('-', d['Program'])]
    validLetters = ['p', 'm', 'a', ':']
    ops = [1,-1]
    timeStringFound=False
    timeBounds=[]
    for r in rs:
        if (r != -1) & (d['Program'][r+1].isnumeric() is not False):
            for o in ops:
                r2=r
                timeBoundFound=False
                numberFound=False
                postFixCount=0
                while timeBoundFound is False:
                    r2+=o
                    if (r2 > -1) & (r2<len(d['Program'])):
                        curLetter = d['Program'][r2].lower()
                        if ((curLetter not in validLetters) and (curLetter.isnumeric() is False)) or ((numberFound) and (curLetter.isnumeric() is False) and (o==-1)) or ((o==1) and (numberFound) and (curLetter.isnumeric() is False) and (postFixCount>2)):
                            if numberFound:
                                timeBoundFound=True
                                if o==-1:
                                    r2=r2-o
                                timeBounds.append(r2)
                        if (curLetter.isnumeric() is False) and (o==1):
                            postFixCount+=1
                        if (curLetter.isnumeric()):
                            numberFound=True
                    else:
                        if numberFound:
                            if o==-1:
                                r2=r2-o
                            timeBoundFound=True
                            timeBounds.append(r2)
    if filetype=='contracts':
        if (len(timeBounds)>1):
            timeString = d['Program'][timeBounds[1]:timeBounds[0]]
            d['Time'] = timeString
            d['Program'] = d['Program'].replace(timeString,'')
    return d

def fixHeaders(row):
    fixCol = -1
    for k in row.keys():
        if row[k] == 'Start Date End Date Description':
            fixCol = k
    if fixCol != -1:
        if(fixCol == 3):
            row[(fixCol-2)] = 'Ch'
            row[(fixCol-1)] = 'Start'
            row[(fixCol)] = 'End'
            row[(fixCol+1)] = 'Description'
            row[(fixCol+2)] = 'Desc2'
        if(fixCol == 2):
            row[(fixCol)] = 'Start'
            row[(fixCol+1)] = 'End'
            row[(fixCol+2)] = 'Description'
    return row
def findHeaderRow(row):
    headers =  { 'Description': ['Start Date End Date Description'],
                    'Inventory Code': ['Amount', 'Start', 'Inventory Code', 'Rate', 'Spots'] }
    for htype in headers:
        for val in row:
            try:
                ind = headers[htype].index(val)
                headers[htype].remove(val)
            except:
                pass
    if(len(headers['Description']) == 0):
        return ['Description', row.name]
    else:
        if(len(headers['Inventory Code']) == 0):
            return ['Inventory Code', row.name]
def classifyReportFormat(res, kw):
    # Process the scraped text
    res = res.split('\n')
    res = [x.replace(' ', '') for x in res ]
    res = [x.replace(':', '') for x in res ]
    res = [x.replace('.', '') for x in res ]
    res = list(filter(lambda a: a != '', res))
    res = res[0:200]
    res = list(set(res))
    # Generate keys from csv and score based on keys
    keyAr = kw['pdftype'].values
    ptypes = {}
    for pt in keyAr:
        ptkw = kw[kw['pdftype']==pt]
        ptypes[pt] = len(ptkw[ptkw['keyword'].isin(res)])
        ptypes[pt] = ptypes[pt]/len(ptkw)
    maxVal = max(ptypes, key=ptypes.get)
    if ptypes[maxVal] > .2:
        return maxVal
    else:
        return False

def dedupeOrderInvoice(o, ia):
    if (o['Program']==None) or (o['Program']!=o['Program']) or (o['Program']==''):
        ia = ia[ia['Rate']==o['Rate']]
    else:
        ia = ia[ia.apply(lambda x: fuzz.token_set_ratio(o['Program'], x['Program'])>80, axis=1)]
    periodstart = o['Date'] - timedelta(days=o['Date'].weekday())
    periodend = periodstart + timedelta(days=6)
    ia = ia[((ia['Date']>=periodstart) & (ia['Date']<=periodend))]
    return True if len(ia)==0 else False
'''
oa = pd.read_csv('testorders.csv')
oa['Date'] = pd.to_datetime(oa['Date'], format='%Y-%m-%d')
ia = pd.read_csv('testinvoices.csv')
ia['Date'] = pd.to_datetime(ia['Date'], format='%Y-%m-%d')
print(len(oa))
oa = oa[oa.apply(dedupeOrderInvoice, axis=1, ia=ia)]
print(oa)
print(len(oa))
'''
#Local News @ 5p M-F M-F 5-530p

## Test whether these match
## M-F 530-6a
## M-F 530-6am

## M-F 5-530pm

def isCycleFolder(s, c):
    fs = s.split('/')
    rv = False#return val
    if 'Political Files' in fs:
        if fs.index('Political Files') != len(fs)-1:
            cycle = fs[(fs.index('Political Files')+1)]
            rv = True if cycle == c else False
    return rv

def isStationFolder(s):
    fs = s.split('/')
    if 'Political Files' in fs:
        if fs.index('Political Files') == len(fs)-1:
            return True
    return False


def preprocessCommitteeNames(p):
    #
    # Preprocess folder names to make it easier to resolve them to their other committees.
    #
    # Get info from name.
    p['NameSimpl'] = p['Name']
    p['Party'] = None

    # Transform words
    p['NameSimpl'] = p['NameSimpl'].str.replace('AZDP',' DEMOCRAT ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('AZRP',' REPUBLICAN', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('AZDEMS',' DEMOCRATIC PARTY ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('AZ ','ARIZONA ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' AZ',' ARIZONA', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' Prty ',' Party ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' ATT ',' ATTORNEY ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' ATY ',' ATTORNEY ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' SEC ',' SECRETARY ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' GEN ',' GENERAL ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' SOS ',' SECRETARY OF STATE ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' SOS',' SECRETARY OF STATE ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' AG ',' ATTORNEY GENERAL ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' AG',' ATTORNEY GENERAL', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' DEM ',' DEMOCRATIC ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' REP ',' REPUBLICAN ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' DEMS ',' DEMOCRATIC PARTY ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' GOV ', ' GOVERNOR ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' PHX ', ' PHOENIX ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('National Republican Senatorial Committee','NRSC', case=False)

    # IE
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace(' IE', '', case=False)

    #PAC
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace(' 4 ', ' For ', case=False)
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace(' ASSC ', ' Association ', case=False)
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('auth', '', case=False)
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('ACLU', 'American Civil Liberties Union', case=False)

    #Candidate
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('For', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 
        'NameSimpl'] = p['NameSimpl'].str.replace('- General Election', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 
        'NameSimpl'] = p['NameSimpl'].str.replace('- Primary Election', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 
        'NameSimpl'] = p['NameSimpl'].str.replace('Phoenix', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 
        'NameSimpl'] = p['NameSimpl'].str.replace('Arizona', '', case=False)

    #Party
    p.loc[p['Name'].str.contains(' R ', case=False), 'Party'] = "Republican"
    p.loc[p['Name'].str.contains(' D ', case=False), 'Party'] = "Democrat"
    p.loc[p['NameSimpl'].str.contains('Republican', case=False), 'Party'] = "Republican"
    p.loc[p['NameSimpl'].str.contains('Democrat', case=False), 'Party'] = "Democrat"
    p['NameSimpl'] = p['NameSimpl'].str.replace(' D ','', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' R ','', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('Democratice', 'Democratic')

    #Federal
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace(' US ', '', case=False)
    #Senate
    p.loc[p['Name'].str.contains('Senate', case=False), 'Race'] = "Senate"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Senate', '', case=False)
    p.loc[p['Name'].str.contains('DSCC', case=False), 'Race'] = "Senate"
    p.loc[p['Name'].str.contains('DSCC', case=False), 'Party'] = "Democrat"
    p.loc[p['Name'].str.contains('NRSC', case=False), 'Race'] = "Senate"
    p.loc[p['Name'].str.contains('NRSC', case=False), 'Party'] = "Republican"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('DSCC', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('NRSC', '', case=False)

    # House
    p.loc[p['Name'].str.contains('House', case=False), 'Race'] = "House"
    p.loc[p['Name'].str.contains('Congress', case=False), 'Race'] = "House"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('House', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Congress', '', case=False)
    p.loc[p['Name'].str.contains('DCCC', case=False), 'Race'] = "House"
    p.loc[p['Name'].str.contains('DCCC', case=False), 'Party'] = "Democrat"
    p.loc[p['Name'].str.contains('NRCC', case=False), 'Race'] = "House"
    p.loc[p['Name'].str.contains('NRCC', case=False), 'Party'] = "Republican"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('NRCC', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('DCCC', '', case=False)

    #State
    p.loc[p['NameSimpl'].str.contains('Arizona Democratic Party', case=False), 'Party'] = "Democrat"
    p.loc[p['NameSimpl'].str.contains('Arizona Republican Party', case=False), 'Party'] = "Republican"
    #Governor
    p.loc[p['NameSimpl'].str.contains('RGA', case=False), 'Party'] = "Republican"
    p.loc[p['NameSimpl'].str.contains('DGA', case=False), 'Race'] = "Governor"
    p.loc[p['NameSimpl'].str.contains('RGA', case=False), 'Race'] = "Governor"
    p.loc[p['NameSimpl'].str.contains('Governor', case=False), 'Race'] = "Governor"
    p.loc[p['NameSimpl'].str.contains('DGA', case=False), 'Party'] = "Democrat"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Governor', '', case=False)
    #AG
    p.loc[p['NameSimpl'].str.contains('Attorney General', case=False), 'Race'] = "Attorney General"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Attorney General', '', case=False)
    p.loc[p['NameSimpl'].str.contains('RAGA', case=False), 'Party'] = "Republican"
    p.loc[p['NameSimpl'].str.contains('DAGA', case=False), 'Race'] = "Attorney General"
    p.loc[p['NameSimpl'].str.contains('RAGA', case=False), 'Race'] = "Attorney General"
    p.loc[p['NameSimpl'].str.contains('DAGA', case=False), 'Party'] = "Democrat"
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('DAGA', ' Democratic Attorneys General Association ', case=False)


    #SOS
    p.loc[p['NameSimpl'].str.contains('Secretary of State', case=False), 'Race'] = "Secretary of State"
    p['NameSimpl'] = p['NameSimpl'].str.replace('Secretary of State', '', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('Secretary State', '', case=False)
    #Superintendent
    p.loc[p['NameSimpl'].str.contains('Superintendent', case=False), 'Race'] = "Superintendent of Public Instruction"
    p['NameSimpl'] = p['NameSimpl'].str.replace('Superintendent of Public Education', ' ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('State Superintendent', '', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('Superintendent', '', case=False)
    # State Treasurer
    p.loc[p['NameSimpl'].str.contains('Treasurer', case=False), 'Race'] = "State Treasurer"
    p['NameSimpl'] = p['NameSimpl'].str.replace('State Treasurer', ' ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('Treasurer', '', case=False)
    #Mayor#
    p.loc[p['NameSimpl'].str.contains('Mayor', case=False), 'Race'] = "Mayor"
    p['NameSimpl'] = p['NameSimpl'].str.replace('Mayor', '', case=False)

    # General vs Primary
    p.loc[(p['Name'].str.contains('ATTORNEY', case=False) == False) & 
        (p['Name'].str.contains('GENERAL', case=False)), 'NameSimpl'] = p['NameSimpl'].str.replace('GENERAL', '', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('PRIMARY','', case=False)

    #All
    p['NameSimpl'] = p['NameSimpl'].str.replace('-',' ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('2018', '')
    p['NameSimpl'] = p['NameSimpl'].str.replace('2016', '')
    p['NameSimpl'] = p['NameSimpl'].str.replace('2014', '')

    #Party Committees
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Democratic Party', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Republican Party', '', case=False)
    #
    # Preprocess folder names to make it easier to resolve them to their other committees.
    #
    # Get info from name.
    p['NameSimpl'] = p['Name']
    p['Party'] = None

    # Transform words
    p['NameSimpl'] = p['NameSimpl'].str.replace('AZDP',' DEMOCRAT ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('AZRP',' REPUBLICAN', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('AZDEMS',' DEMOCRATIC PARTY ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('AZ ','ARIZONA ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' AZ',' ARIZONA', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' Prty ',' Party ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' ATT ',' ATTORNEY ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' ATY ',' ATTORNEY ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' SEC ',' SECRETARY ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' GEN ',' GENERAL ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' SOS ',' SECRETARY OF STATE ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' SOS',' SECRETARY OF STATE ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' AG ',' ATTORNEY GENERAL ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' AG',' ATTORNEY GENERAL', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' DEM ',' DEMOCRATIC ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' REP ',' REPUBLICAN ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' DEMS ',' DEMOCRATIC PARTY ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' GOV ', ' GOVERNOR ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' PHX ', ' PHOENIX ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('National Republican Senatorial Committee','NRSC', case=False)

    # IE
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace(' IE', '', case=False)

    #PAC
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace(' 4 ', ' For ', case=False)
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace(' ASSC ', ' Association ', case=False)
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('auth', '', case=False)
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('ACLU', 'American Civil Liberties Union', case=False)

    #Candidate
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('For', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 
        'NameSimpl'] = p['NameSimpl'].str.replace('- General Election', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 
        'NameSimpl'] = p['NameSimpl'].str.replace('- Primary Election', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 
        'NameSimpl'] = p['NameSimpl'].str.replace('Phoenix', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 
        'NameSimpl'] = p['NameSimpl'].str.replace('Arizona', '', case=False)

    #Party
    p.loc[p['Name'].str.contains(' R ', case=False), 'Party'] = "Republican"
    p.loc[p['Name'].str.contains(' D ', case=False), 'Party'] = "Democrat"
    p.loc[p['NameSimpl'].str.contains('Republican', case=False), 'Party'] = "Republican"
    p.loc[p['NameSimpl'].str.contains('Democrat', case=False), 'Party'] = "Democrat"
    p['NameSimpl'] = p['NameSimpl'].str.replace(' D ','', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace(' R ','', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('Democratice', 'Democratic')

    #Federal
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace(' US ', '', case=False)
    #Senate
    p.loc[p['Name'].str.contains('Senate', case=False), 'Race'] = "Senate"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Senate', '', case=False)
    p.loc[p['Name'].str.contains('DSCC', case=False), 'Race'] = "Senate"
    p.loc[p['Name'].str.contains('DSCC', case=False), 'Party'] = "Democrat"
    p.loc[p['Name'].str.contains('NRSC', case=False), 'Race'] = "Senate"
    p.loc[p['Name'].str.contains('NRSC', case=False), 'Party'] = "Republican"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('DSCC', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('NRSC', '', case=False)

    # House
    p.loc[p['Name'].str.contains('House', case=False), 'Race'] = "House"
    p.loc[p['Name'].str.contains('Congress', case=False), 'Race'] = "House"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('House', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Congress', '', case=False)
    p.loc[p['Name'].str.contains('DCCC', case=False), 'Race'] = "House"
    p.loc[p['Name'].str.contains('DCCC', case=False), 'Party'] = "Democrat"
    p.loc[p['Name'].str.contains('NRCC', case=False), 'Race'] = "House"
    p.loc[p['Name'].str.contains('NRCC', case=False), 'Party'] = "Republican"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('NRCC', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('DCCC', '', case=False)

    #State
    p.loc[p['NameSimpl'].str.contains('Arizona Democratic Party', case=False), 'Party'] = "Democrat"
    p.loc[p['NameSimpl'].str.contains('Arizona Republican Party', case=False), 'Party'] = "Republican"
    #Governor
    p.loc[p['NameSimpl'].str.contains('RGA', case=False), 'Party'] = "Republican"
    p.loc[p['NameSimpl'].str.contains('DGA', case=False), 'Race'] = "Governor"
    p.loc[p['NameSimpl'].str.contains('RGA', case=False), 'Race'] = "Governor"
    p.loc[p['NameSimpl'].str.contains('Governor', case=False), 'Race'] = "Governor"
    p.loc[p['NameSimpl'].str.contains('DGA', case=False), 'Party'] = "Democrat"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Governor', '', case=False)
    #AG
    p.loc[p['NameSimpl'].str.contains('Attorney General', case=False), 'Race'] = "Attorney General"
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Attorney General', '', case=False)
    p.loc[p['NameSimpl'].str.contains('RAGA', case=False), 'Party'] = "Republican"
    p.loc[p['NameSimpl'].str.contains('DAGA', case=False), 'Race'] = "Attorney General"
    p.loc[p['NameSimpl'].str.contains('RAGA', case=False), 'Race'] = "Attorney General"
    p.loc[p['NameSimpl'].str.contains('DAGA', case=False), 'Party'] = "Democrat"
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('DAGA', ' Democratic Attorneys General Association ', case=False)


    #SOS
    p.loc[p['NameSimpl'].str.contains('Secretary of State', case=False), 'Race'] = "Secretary of State"
    p['NameSimpl'] = p['NameSimpl'].str.replace('Secretary of State', '', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('Secretary State', '', case=False)
    #Superintendent
    p.loc[p['NameSimpl'].str.contains('Superintendent', case=False), 'Race'] = "Superintendent of Public Instruction"
    p['NameSimpl'] = p['NameSimpl'].str.replace('Superintendent of Public Education', ' ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('State Superintendent', '', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('Superintendent', '', case=False)
    # State Treasurer
    p.loc[p['NameSimpl'].str.contains('Treasurer', case=False), 'Race'] = "State Treasurer"
    p['NameSimpl'] = p['NameSimpl'].str.replace('State Treasurer', ' ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('Treasurer', '', case=False)
    #Mayor#
    p.loc[p['NameSimpl'].str.contains('Mayor', case=False), 'Race'] = "Mayor"
    p['NameSimpl'] = p['NameSimpl'].str.replace('Mayor', '', case=False)

    # General vs Primary
    p.loc[(p['Name'].str.contains('ATTORNEY', case=False) == False) & 
        (p['Name'].str.contains('GENERAL', case=False)), 'NameSimpl'] = p['NameSimpl'].str.replace('GENERAL', '', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('PRIMARY','', case=False)

    #All
    p['NameSimpl'] = p['NameSimpl'].str.replace('-',' ', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('2018', '')
    p['NameSimpl'] = p['NameSimpl'].str.replace('2016', '')
    p['NameSimpl'] = p['NameSimpl'].str.replace('2014', '')

    #Party Committees
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Democratic Party', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('Republican Party', '', case=False)
    p.loc[(p['Jurisdiction'] != 'Non-Candidate Issue Ads'),
        'NameSimpl'] = p['NameSimpl'].str.replace('Republican', '', case=False)
    p.loc[(p['Jurisdiction'] != 'Non-Candidate Issue Ads'),
        'NameSimpl'] = p['NameSimpl'].str.replace('Democrat', '', case=False)
    p.loc[(p['Jurisdiction'] != 'Non-Candidate Issue Ads'),
        'NameSimpl'] = p['NameSimpl'].str.replace('Party', '', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('Behalf of', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('auth', '', case=False)

    # Put everything that's local in state because stations can't agree on what's what.
    p.loc[p['Jurisdiction']=='Local', 'Jurisdiction'] = "State"

    # Acronym Expansion
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('PFAW', 'People for the American Way', case=False)
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('RGA', 'Republican Governors Association', case=False)
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('DCCC', 'Democratic Congressional Campaign Committee', case=False)
    p['Cycle'] = p['Cycle'].astype(str)
    p = p.replace({np.nan:None})
    p['NameSimpl'] = p['NameSimpl'].str.strip()
    p.loc[(p['Jurisdiction'] != 'Non-Candidate Issue Ads'),
        'NameSimpl'] = p['NameSimpl'].str.replace('Democrat', '', case=False)
    p.loc[(p['Jurisdiction'] != 'Non-Candidate Issue Ads'),
        'NameSimpl'] = p['NameSimpl'].str.replace('Party', '', case=False)
    p['NameSimpl'] = p['NameSimpl'].str.replace('Behalf of', '', case=False)
    p.loc[p['Jurisdiction'] != 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('auth', '', case=False)

    # Put everything that's local in state because stations can't agree on what's what.
    p.loc[p['Jurisdiction']=='Local', 'Jurisdiction'] = "State"

    # Acronym Expansion
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('PFAW', 'People for the American Way', case=False)
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('RGA', 'Republican Governors Association', case=False)
    p.loc[p['Jurisdiction'] == 'Non-Candidate Issue Ads', 'NameSimpl'] = p['NameSimpl'].str.replace('DCCC', 'Democratic Congressional Campaign Committee', case=False)
    p['Cycle'] = p['Cycle'].astype(str)
    p = p.replace({np.nan:None})
    p['NameSimpl'] = p['NameSimpl'].str.strip()
    return p