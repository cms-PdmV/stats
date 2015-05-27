#!/usr/bin/python

import json
import pycurl
from collections import OrderedDict
from cStringIO import StringIO


def compareDS(t1, t2):
    def tierP(t):
        tierPriority=[
            '/RECO',
            'SIM-RECO',
            'DIGI-RECO',
            'AOD',
            'SIM-RAW-RECO',
            'DQM' ,
            'GEN-SIM',
            'RAW-RECO',
            'USER',
            'ALCARECO']
        for (p, tier) in enumerate(tierPriority):
            if tier in t:
                return p
        return t
    p1 = tierP(t1)
    p2 = tierP(t2)
    decision = (p1 > p2)
    return decision * 2 - 1


class GetTiers():

    def __init__(self):
        self.github_raw_url = ('https://raw.githubusercontent.com/dmwm/DBS/'+
                               '5d59456a0ce774e45e069313721571eb63f08a7c/'+
                               'Schema/DDL/initialize-template.sql')
        self.uppercut = ('-- INSERT INTO DATA_TIERS (DATA_TIER_NAME, ' +
                         'CREATION_DATE, CREATE_BY) VALUES (?, ?, ?);')
        self.downcut = '-- DATASET_TYPES'

    def curl(self, url):
        out = StringIO()
        curl = pycurl.Curl()
        curl.setopt(pycurl.URL, str(url))
        curl.setopt(pycurl.WRITEFUNCTION, out.write)
        curl.setopt(pycurl.SSL_VERIFYPEER, 0)
        curl.setopt(pycurl.SSL_VERIFYHOST, 0)
        curl.perform()
        try:
            return out.getvalue(), curl.getinfo(curl.RESPONSE_CODE)
        except Exception:
            print "Error %s/n" % curl.getinfo(curl.RESPONSE_CODE)

    def dbs_tiers(self):
        tiers = []
        page, status = self.curl(self.github_raw_url)
        page = page.split(self.uppercut)[1]
        page = page.split(self.downcut)[0]
        for line in page.strip().split(';'):
            if line == '':
                continue
            tiers.append(line.split('VALUES')[1].split(',')[0].replace("'", "")
                         .replace('(', '').strip())
        return tiers

# extend with what is in McM
mcm_tiers = ['GEN-SIM', 'AODSIM', 'GEN-SIM-RAW', 'GEN', 'MINIAODSIM', 'DQMIO',
             'DQM', 'GEN-SIM-RECO', 'GEN-SIM-RECODEBUG', 'ALCARECO',
             'GEN-SIM-RAW-RECO', 'GEN-RAW', 'GEN-SIM-RAWDEBUG',
             'GEN-SIM-DIGI-RAW', 'GEN-RAWDEBUG', 'LHE', 'RECO', 'PREMIX-RAW']

tiers = list(set(GetTiers().dbs_tiers() + mcm_tiers))
tiers.sort(cmp=compareDS)
for t in tiers:
    print t
objt = {}
for i, d in enumerate(tiers):
    objt[d] = i*5
print json.dumps(objt)

