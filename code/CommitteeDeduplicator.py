import pandas as pd
import requests
import os, sys
from fuzzywuzzy import fuzz
import numpy as np
import shutil

matched = []
curName = ""
committeeCount=0
uniqueCom = pd.DataFrame(columns=['Cycle', 'District', 'Jurisdiction', 
                                  'Name', 'Path', 'Race', 'Station', 'Party',
                                 'NameSimpl'])
dedupedCom = pd.DataFrame(columns=['Cycle', 'District', 'Jurisdiction', 
                                  'Name', 'Path', 'Race', 'Station', 'Party',
                                 'NameSimpl', 'MatchedName', 'MatchedStation'])
def linkDuplicateCommittees(p):
    # Use fuzzy string matching to link the same committees accross stations.
    def matcher(r):
        global uniqueCom
        global dedupedCom
        matcher = {'Race': None, 'District': None, 'Party': None}
        def closestNames(n):
            nonlocal matcher
            matchVal = fuzz.token_sort_ratio(n.NameSimpl, r.NameSimpl)
            matchVal2 = fuzz.token_set_ratio(n.NameSimpl, r.NameSimpl)
            if r['Jurisdiction'] == 'Non-Candidate Issue Ads':
                matchThreshold = 75
            else:
                matchThreshold = 78
            # Potentially average sort and set for matchVal rate.
            if (matchVal+matchVal2)/2 > matchThreshold:
                if (n.Race is not None) & (matcher['Race'] is None):
                    matcher['Race'] = n.Race
                if (n.District is not None) & (matcher['District'] is None):
                    matcher['District'] = n.District
                if (n.Party is not None) & (matcher['Party'] is None):
                    matcher['Party'] = n.Party
                return True
            else:
                return False
        def fixDupComs(match, replace):
            global uniqueCom
            global dedupedCom
            print("Fixing duplicate committees...")
            print(match['NameSimpl'].values)
            print(r['NameSimpl'])
            # Replace already matched committees with better committee name.
            dedupedCom.loc[dedupedCom['MatchedName'].isin(match['Name'].values), 'MatchedName'] = replace['Name']
            badindexes = uniqueCom[uniqueCom['Name'].isin(match['Name'].values)].index.values
            uniqueCom = uniqueCom.drop(badindexes)
        possmatch = uniqueCom[(uniqueCom.Cycle==r.Cycle)&(uniqueCom.Jurisdiction==r.Jurisdiction)]
        match = possmatch[possmatch.apply(closestNames, axis=1)]
        # Check already matched committees.
        if(len(match) == 0):
            possmatch = dedupedCom[(dedupedCom.Cycle==r.Cycle)&(dedupedCom.Jurisdiction==r.Jurisdiction)]
            match = possmatch[possmatch.apply(closestNames, axis=1)]
            if len(match)>0:
                match['Name'] = match['MatchedName']
                match['Station'] = match['MatchedStation']
                match = uniqueCom[uniqueCom['Name'].isin(match[0:1]['MatchedName'].values)][0:1]
        if len(match) == 1:
            if (matcher['Race'] is None) & (r['Race'] is not None):
                uniqueCom.loc[match.index.values, 'Race'] = r['Race']
            if (matcher['District'] is None) & (r['District'] is not None):
                uniqueCom.loc[match.index.values, 'District'] = r['District']
            if (matcher['Party'] is None) & (r['Party'] is not None):
                uniqueCom.loc[match.index.values, 'Party'] = r['Party']
            r['MatchedName'] = match[0:1]['Name'].values[0]
            r['MatchedStation'] = match[0:1]['Station'].values[0]
        else:
            if(len(match['Name'].unique())>1):
                print(match['Name'].unique())
                fixDupComs(match, r)
            r['MatchedName'] = r['Name']
            r['MatchedStation'] =r['Station']
            uniqueCom = uniqueCom.append(r)
        dedupedCom = dedupedCom.append(r)
        return r
    pz = p.apply(matcher, axis=1)
    return pz

def createAdComKey(uniqueCom, dedupedCom): # Merge in deduped committee primary keys, then write committees key to disk.
    coms18 = uniqueCom[uniqueCom['Cycle']=='2018'].sort_values(by='NameSimpl').reset_index(drop=True)
    coms18['id'] = coms18.index
    coms18['MatchedName'] = coms18['Name']
    dedupedCom2=dedupedCom[dedupedCom['Cycle']=="2018"][1:].merge(coms18[['id', 'MatchedName']], 
                                                                how='left', on=['MatchedName'])
    dedupedCom2.to_csv('adcommitteekey.csv', index=False)
    uniqueCom[(uniqueCom['Race'].isna()) | (uniqueCom['Party'].isna())].to_csv('comsNeedingInfo.csv')
    print('Wrote adcommittee key and committees needing info to CSV. Fill in the missing info before you move onto the next step.')

def mergeIECoding(): # Merge in hand coded IE spends and sides.
    iers = pd.read_csv('ies.csv')
    ies = coms18[(coms18['Jurisdiction'] == 'Non-Candidate Issue Ads') & (coms18['Cycle']=='2018')]
    ies = ies[['Name', 'MatchedName', 'Station', 'id', 'NameSimpl']].merge(iers.drop('NameSimpl', axis=1), how="left", on=['Name', 'Station'])
    uniqueComs2 = coms18[(coms18['Jurisdiction'] != 'Non-Candidate Issue Ads') & 
                        (coms18['Cycle'] == '2018')].append(ies)
    uniqueComs2.to_csv('uniqueComPreFinal.csv')

def finalizeCommitteeUpload():
    # Write to CSV todo manual final edits
    # Write final unique committees to csv for database upload
    finalComs = pd.read_csv('uniqueComPreFinal.csv')
    comsWithInfo = pd.read_csv('comsNeedingInfo.csv')
    finalComs = finalComs[(finalComs['Race'].isna() == False) & (finalComs['Party'].isna() == False)]
    finalComs = finalComs.append(comsWithInfo)
    finalComs = finalComs.drop(['Unnamed: 0', 'id'], axis=1)
    finalComs = finalComs.sort_values(by='NameSimpl', na_position='first').reset_index(drop=True)
    finalComs['id'] = finalComs.index
    finalComs.columns = map(str.lower, finalComs.columns)
    finalComs = finalComs.drop(['matchedname', 'name', 
                    'station', 'path', 'matchedstation'], axis=1)
    finalComs.rename(columns={'namesimpl':'name',
                                'party':'side',
                                'jurisdiction':'juris'}).to_csv('comsupload.csv', index=False)

def mergeInStationPACids():
    key = pd.read_csv('adcommitteekey.csv')
    ads = pd.read_csv('scrapedads.csv')
    stations = pd.read_csv('../results/stationsPhoenixdma.csv')
    ads = ads[ads['Station']!='KTVW-DT']
    # Merge in commitees
    key = key.rename(columns={'Name':'Committee', 'id':'pacid'})
    key = key[['Committee','pacid']]
    key = key.drop_duplicates()
    print(key)
    # Running on empty directories fails the pac search for AZ09, pac search would pick this up on your desktop.
    ads.loc[ads.Committee == 'AZ 09', 'Committee'] = 'Greg Stanton for Congress'
    ads = ads.merge(key[['Committee', 'pacid']], on='Committee', how='left').drop(['Committee'], axis=1)
    ads['pacid'] = ads['pacid'].astype(int)
    print(len(ads[ads['pacid']==37]))
    # Merge in stations
    ads = ads.rename(columns={'Station':'sign'})
    stations = stations.rename(columns={'id':'stationid'})
    ads = ads.merge(stations[['stationid', 'sign']], on="sign").drop(['sign'], axis=1)
    # Map columns to database schema, write to file.
    ads = ads.rename(columns={'Date':'airdate', 'Time': 'airtime'})
    ads.columns = map(str.lower, ads.columns)
    ads['cycle'] = ads['cycle'].astype(int)
    ads.to_csv('../results/adsupload.csv', index=False)

mergeInStationPACids()