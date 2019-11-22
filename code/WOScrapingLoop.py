import camelot
import pandas as pd
import numpy as np
import pdfminer as pm
import math
from datetime import datetime, timedelta  
import warnings
import os
import subprocess
import ocrmypdf
import sys
from fuzzywuzzy import fuzz
import traceback
import re
import time
from utils import convertPDFToTable, pacFromPath, classifyReportFormat, dedupeOrderInvoice, isCycleFolder, isStationFolder
from WOOrderContractScraper import processOrder
from WOInvoiceScraper import processInvoice


def parseCyclePDFs(cyc, basepath, test):
    fulladlist = pd.DataFrame(columns=['Date', 'Rate', 'Committee', 'Station', 'Program'])
    pdfResults = pd.DataFrame(columns=['Path', 'Result'])
    if os.path.exists('./scrapedads.csv'):
        fulladlist = pd.read_csv('scrapedads.csv')
    if os.path.exists('./pdfresults.csv'):
        pdfResults = pd.read_csv('pdfresults.csv')
    kw = pd.read_csv('/media/andrew/F08C9B848C9B444E/analysis/tv/fccscraper/keys/filetypekeywords.csv') # keywords
    malads = pd.DataFrame(columns=['Start Date', 'End Date', 'Weekdays',
                                   'Spots/Week', 'Rate', 'Rating'])
    def digToNextLevel(folderpath, name, files):
        nonlocal pdfResults
        nonlocal fulladlist
        nonlocal kw
        folderinfo = pacFromPath(folderpath)
        adtimes = [] # Final add array
        orderads = pd.DataFrame(columns=['Start Date', 'End Date', 'Weekdays',
                                           'Spots/Week', 'Rate', 'Rating', 'Time'])
        invoiceads = pd.DataFrame(columns=['Date', 'Rate', 'Committee', 'Station', 'Program', 'Time'])
        def expandDays(x, station, name):
            adStartDate = datetime.strptime(x['Start Date'], '%m%d%y')
            adStartDate = adStartDate - timedelta(days=adStartDate.weekday())
            # Test if adbuy string is malformed.
            for i, day in enumerate(x['Weekdays']):
                if(day!='-'):
                    if(day.isdigit()):
                        for j in range(0, int(day)):
                            adtimes.append({
                                        'Date': (adStartDate + timedelta(days=i)),  
                                        'Rate': x['Rate'],
                                        'Committee': name,
                                        'Station': station,
                                        'Time': x['Time'],
                                        'Program': x['Program']
                                       })
                    else: ## CODE FOR MTW NOTATION
                        adtimes.append({
                                        'Date': (adStartDate + timedelta(days=i)),  
                                        'Rate': x['Rate'],
                                        'Committee': name,
                                        'Station': station,
                                        'Time': x['Time'],
                                        'Program': x['Program']
                                       })

        def checkIntegrity(ad, type): # Checks ads are in valid format, performs common corrections due to OCR errors.
            nonlocal orderads
            nonlocal invoiceads
            dateCols = {'orders': ['Start Date', 'End Date'],'contracts': ['Start Date', 'End Date'], 'invoices': ['Date']}
            malformed=False
            # Date
            for d in dateCols[type]:
                ad[d] = ad[d].replace('/', '')
                ad[d] = ad[d].replace('o', '0')
                ad[d] = ad[d].replace('O', '0')
                ad[d] = ad[d].replace('g', '9')
                try:
                    datetime.strptime(ad[d], '%m%d%y')
                except:
                    malformed=True            
            # Rate
            ad['Rate'] = ad['Rate'].split('$')[1]
            ad['Rate'] = re.sub(r"[^0-9]",'', ad['Rate'])
            try:
                ad['Rate'] = ad['Rate'][:-2]
                ad['Rate'] = float(ad['Rate'])
            except:
                malformed=True
            # Weekday
            if (type == 'orders') or (type=='contracts'):
                validDateChars=['M', 'T', 'W', 'h', 'F', 'S', 'a', 'u', 'H', 'A', 'U']
                if len(ad['Weekdays']) != 7:
                    malformed=True
                for char in ad['Weekdays']:
                    if char.isalpha():
                        if char not in validDateChars:
                            malformed=True
            if malformed!=True:
                if (type == 'orders') or (type=='contracts'):
                    orderads = orderads.append(ad, ignore_index=True, sort=False)
                else:
                    invoiceads = invoiceads.append(ad, ignore_index=True, sort=False)
            # Maybe malformed sheet else?
        def processPDF(item, station, name):
            print(time.ctime(time.time()) + ' - ' + station + ' - ' + name + ' - ' + item.split('/')[-1])
            nonlocal pdfResults
            nonlocal fulladlist
            pdfProcessors = {'orders': processOrder, 'contracts': processOrder, 'invoices': processInvoice}
            pdfCols = {'orders': '35,52,97,136,210,256,308,360,390,422,456,469,490,513,550', 'contracts': '35,52,97,136,210,256,308,360,390,422,456,469,490,513,550','invoices': '62, 92, 114, 154, 194, 287, 311, 384, 414, 524, 573'}
            result = subprocess.run(['pdftotext', item, '-'], 
                                        stdout=subprocess.PIPE).stdout.decode()
            if((len(result.split('\n')[0:30])> 5)):
                reportFormat = classifyReportFormat(result, kw)
                if (reportFormat != False) & (reportFormat in pdfProcessors != False) :
                    try:
                        pdfTable = convertPDFToTable(item, pdfCols[reportFormat])
                        ads = pdfProcessors[reportFormat](pdfTable, name, station, reportFormat)
                        if len(ads) > 0:
                            ads.apply(checkIntegrity, axis=1, type=reportFormat)
                            pdfResults = pdfResults.append({'Path': item, 
                                                            'Result': reportFormat+' Success'}, ignore_index=True, sort=False)
                        else:
                            pdfResults = pdfResults.append({'Path': item, 
                                            'Result': reportFormat+' Scraping Error'}, ignore_index=True)
                    except:
                        #traceback.print_exc()
                        pdfResults = pdfResults.append({'Path': item, 
                                            'Result': reportFormat+'Scraping or Timeout Error'}, ignore_index=True)
                else:
                    if reportFormat != False:
                        pdfResults = pdfResults.append({'Path': item, 
                           'Result': reportFormat+'No parser written for file.'}, ignore_index=True)
                    else:
                        pdfResults = pdfResults.append({'Path': item, 
                           'Result': 'No parser written for file.'}, ignore_index=True)
            else:
                if len(result.split('\n')[0:30])==1:
                    try: 
                        ocrmypdf.ocr(item, item, deskew=True, rotate_pages=True)
                        processPDF(item, station, name)
                    except:
                        pdfResults = pdfResults.append({'Path': item, 'Result': 'OCR Error'}, ignore_index=True)
                else:
                    pdfResults = pdfResults.append({'Path': item, 
                   'Result': 'No parser written for file.'}, ignore_index=True)
        for item in files:
            if (pdfResults['Path'].str.contains(os.path.join(folderpath, item), regex=False).sum()==0) or (test):
                processPDF((os.path.join(folderpath, item)), folderinfo['station'], folderinfo['pac'])
        if (len(orderads)>0) or (len(invoiceads)>0):
            orderads = orderads.drop_duplicates() # subset array arg to ignore columns
            orderads.apply(expandDays, axis=1, station=folderinfo['station'], name=folderinfo['pac'])
            oa = pd.DataFrame(adtimes) #Order ads
            ia = pd.DataFrame(invoiceads) # Invoice ads
            ia['Date'] = pd.to_datetime(ia['Date'], format='%m%d%y')
            if (len(adtimes)>0) & (len(invoiceads)>0): # Deduplicate between both forms
                oa = oa[oa.apply(dedupeOrderInvoice, axis=1, ia=ia)]
            invoiceads = invoiceads.drop_duplicates()
            fulladlist = fulladlist.append(oa, ignore_index=True, sort=False)
            fulladlist = fulladlist.append(ia, ignore_index=True, sort=False)
            fulladlist['Cycle'] = cyc
            fulladlist.to_csv('scrapedads.csv', index=False)
            pdfResults.to_csv('pdfresults.csv', index=False)
    for dirName, subdirList, fileList in os.walk(basepath):
        if (isCycleFolder(dirName, cyc)) & (len(fileList)>0):
            digToNextLevel(dirName, cyc, fileList)
        else:
            if isStationFolder(dirName):
                dn = dirName.split('/')
                print(dn[-2])
    return {'ads': fulladlist, 'pdfs': pdfResults, 'malformedads': malads}
print('test')
results = parseCyclePDFs('2018', '/media/andrew/F08C9B848C9B444E/analysis/tv/buys/PHOENIX (PRESCOTT)/', test=False)
#results['ads'].to_csv('scrapedads.csv', index=False)
#results['pdfs'].to_csv('pdfresults.csv', index=False)
## Local News @ 5p M-F M-F 5-530p