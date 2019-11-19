import camelot
import pandas as pd
import numpy as np
from utils import convertPDFToTable
import os
import time
import timeout_decorator



@timeout_decorator.timeout(300, use_signals=False)
def processInvoice(tables, committee, station, reportformat):
    ads = pd.DataFrame(columns=['Air Date', 'Air Time', 'Rate', 'Description'])
    def findHeaderRow(row):
        headers =  { 'InventoryCode': ['Air Date', 'Air Time', 'Description', 'Rate'] }
        for htype in headers:
            for val in row:
                try:
                    ind = headers[htype].index(val)
                    headers[htype].remove(val)
                except:
                    pass
        if(len(headers['InventoryCode']) == 0):
            return ['InventoryCode', row.name]
    for t in tables:
        progColName=False
        headerConfig = t.apply(findHeaderRow, axis=1).dropna()
        if len(headerConfig)>0:
            progColName = headerConfig.values[0][0]
        if progColName != False:
            progs = t.copy()
            # Set column headers to be the header row, remove row from data.
            progs.columns = progs.iloc[headerConfig.values[0][1]]
            progs = progs.iloc[headerConfig.values[0][1]:]
            progs = progs.reset_index(drop=True)
            progs = progs.iloc[1:] # Remove residual header row.
            progs = progs.replace('', np.nan, regex=True)
            progs = progs[progs['Air Date']!='Air Date']
            progs = progs[['Air Date', 'Air Time', 'Description', 'Rate']]
            progs = progs.dropna()
            ads = ads.append(progs, ignore_index=True, sort=False)
    ads = ads.drop_duplicates()
    ads = ads.rename(columns={'Air Date': 'Date', 'Air Time': 'Time', 'Description': 'Program'})
    ads['Station'] = station
    ads['Committee'] = committee
    return ads


# Testing loop
'''
folderpath = "/media/andrew/F08C9B848C9B444E/analysis/tv/fccscraper/tests/invoices/"
for item in list(os.scandir(folderpath)):
    print(item.name)
    tbl = convertPDFToTable(folderpath+item.name, '62, 92, 114, 154, 194, 287, 311, 384, 414, 524, 573')
    tbl = processInvoice(tbl, "Test for Arizona", "KAND")
    print(tbl)
    '''