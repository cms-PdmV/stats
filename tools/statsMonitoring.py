#! /usr/bin/env python

#dbs3_url = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/'
#rqmgr2_url = 'https://cmsweb-testbed.cern.ch/reqmgr2/data/request'
dbs3_url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/'
rqmgr2_url = 'https://cmsweb.cern.ch/reqmgr2/data/request'

#-------------------------------------------------------------------------------
import sys

req_version = (2,6)
cur_version = sys.version_info

if cur_version < req_version:
    print "At least Python 2.6 is required!"
    sys.exit(1)

#-------------------------------------------------------------------------------

import os
import httplib
import urllib2
import time
import datetime
import random
import json

from phedex import phedex, runningSites, custodials, atT2, atT3

##define global variables
# Collect all the requests which are in one of these stati which allow for
priority_changable_stati = ['new','assignment-approved']
skippable_stati = ["rejected", "aborted", "failed", "rejected-archived",
        "aborted-archived", "failed-archived", "aborted-completed"]

complete_stati = ["announced", "rejected", "aborted", "failed", "normal-archived",
        "aborted-archived", "failed-archived"]

# Types of requests
request_types = ['MonteCarlo', 'MonteCarloFromGEN', 'ReDigi', 'ReReco', 'Resubmission']

##TO-DO do we need this anymore?
wma2reason = {'success':'success', # wma2reason by vincenzo spinoso!
        'failure':'failure', 'Pending':'pending', 'Running':'running', 'cooloff':'cooloff',
        'pending':'queued', 'inWMBS':'inWMBS', 'total_jobs':'total_jobs', 'inQueue' : 'inQueue'}

def timer(method):
    def timed(*args, **kw):
        t0 = time.time()
        result = method(*args, **kw)
        t1 = time.time()
        if t1-t0 > 100:
            print "%s ####method took: %s" % (method.__name__, t1-t0)
        return result
    return timed

#-------------------------------------------------------------------------------
# Needed for authentication

class X509CertAuth(httplib.HTTPSConnection):
    '''Class to authenticate via Grid Certificate'''
    def __init__(self, host, *args, **kwargs):
        key_file = None
        cert_file = None

        x509_path = os.getenv("X509_USER_PROXY", None)
        if x509_path and os.path.exists(x509_path):
            key_file = cert_file = x509_path

        if not key_file:
            x509_path = os.getenv("X509_USER_KEY", None)
            if x509_path and os.path.exists(x509_path):
                key_file = x509_path

        if not cert_file:
            x509_path = os.getenv("X509_USER_CERT", None)
            if x509_path and os.path.exists(x509_path):
                cert_file = x509_path

        if not key_file:
            x509_path = os.getenv("HOME") + "/.globus/userkey.pem"
            if os.path.exists(x509_path):
                key_file = x509_path

        if not cert_file:
            x509_path = os.getenv("HOME") + "/.globus/usercert.pem"
            if os.path.exists(x509_path):
                cert_file = x509_path

        if not key_file or not os.path.exists(key_file):
            print >>stderr, "No certificate private key file found"
            exit(1)

        if not cert_file or not os.path.exists(cert_file):
            print >>stderr, "No certificate public key file found"
            exit(1)

        httplib.HTTPSConnection.__init__(self,  host,key_file = key_file,cert_file = cert_file,**kwargs)

#-------------------------------------------------------------------------------

class X509CertOpen(urllib2.AbstractHTTPHandler):
    def default_open(self, req):
        return self.do_open(X509CertAuth, req)

#-------------------------------------------------------------------------------
def eval_wma_string(string):
    string = string.replace("null",'None')
    string = string.replace("true","True")
    string = string.replace("false","False")
    return eval(string)

#-------------------------------------------------------------------------------

def generic_get(url, do_eval=True):
    opener = urllib2.build_opener(X509CertOpen())
    datareq = urllib2.Request(url)

    datareq.add_header('Accept', 'application/json')
    datareq.add_header('authenticated_wget', "The ultimate wgetter")

    requests_list_str = opener.open(datareq).read()
    ret_val = requests_list_str

    if do_eval:
        ret_val = eval_wma_string(requests_list_str)

    return ret_val
#-------------------------------------------------------------------------------

def generic_post(url, data_input):

    data = json.dumps(data_input)
    print data
    print url
    opener = urllib2.build_opener(X509CertOpen())
    datareq = urllib2.Request(url, data, {"Content-type" : "application/json"})
    requests_list_str = opener.open(datareq).read()
    ret_val = requests_list_str

    return ret_val
#-------------------------------------------------------------------------------

def get_requests_list(pattern="", not_in_wmstats=False, newest=False):

    if not_in_wmstats:
        print "###ERROR## not in wmstats. Old method was removed..."

    opener = urllib2.build_opener(X509CertOpen())
    if newest:
        ## lets get todays date and get -1 year from now
        today_str = "{d.year}-{d.month}-{d.day}".format(d=datetime.date.today())
        today_str_split = today_str.split("-")
        year_ago_str = "%s-%s-%s" % (int(today_str_split[0])-1, today_str_split[1], today_str_split[2])
        year_ago_split = year_ago_str.split("-")

        ##query for worflows from 1 year ago to today
        url = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bydate?startkey=[%s,%s,%s,0,0,0]&endkey=[%s,%s,%s,0,0,0]" % (
            year_ago_split[0], year_ago_split[1], year_ago_split[2], today_str_split[0], today_str_split[1], today_str_split[2])
    else:
      ##we default to old one
        url = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtype"
    datareq = urllib2.Request(url)
    datareq.add_header('authenticated_wget', "The ultimate wgetter")
    print "Getting the list of requests from %s..." % (url)
    requests_list_str = opener.open(datareq).read()
    print " Got it in %s Bytes" % len(requests_list_str)
    data = json.loads(requests_list_str)

    if newest:
        req_list = map(lambda x: {"request_name" : x["value"]["RequestName"], "status" : x["value"]["RequestStatus"],
                "type" : x["value"]["RequestType"]}, data["rows"])

    else:
        req_list= map( lambda item : {"request_name" : item[0], "status" : item[1],
                "type" :item[2]}, map(lambda r : r['key'] , data['rows']))

    print "total number of requests from wmstats: %s" % (len(req_list))
    return req_list

#-------------------------------------------------------------------------------

def get_dataset_name(dataset_list):
    """
    takes output_datasets list and sorts it in prioritized way.
    returns: first ouput dataset, output_dataset list
    """
    dataset = ''
    apossibleChoice = ''

    def compareDS(s1, s2):
        t1 = s1.split('/')[1:]
        t2 = s2.split('/')[1:]
        def tierP(t):
            tierPriority = [
                            '/RECO',
                            'SIM-RECO',
                            'DIGI-RECO',
                            'AOD',
                            'SIM-RAW-RECO',
                            'GEN-SIM',
                            'RAW-RECO',
                            'USER',
                            'ALCARECO',
                            'DQM'
                            ]
            for (p,tier) in enumerate(tierPriority):
                if tier in t:
                    # print t,p
                    return p
            # print t
            return t

        p1 = tierP(t1[2])
        p2 = tierP(t2[2])

        decision = (p1 > p2)
        #print t1,t2,decision
        return decision*2 - 1

    dataset_list.sort(cmp=compareDS)

    if len(dataset_list) == 0:
        dataset = 'None Yet'
    else:
        dataset = dataset_list[0]

    if 'None-None' in dataset or 'None-' in dataset:
        dataset = 'None Yet'

    return dataset,dataset_list

def configsFromWorkload(workload):
    ##TO-DO: What about MC requests configs???
    res = []
    if workload['RequestType'] == 'TaskChain':
        i = 1
        num_of_tasks = workload['TaskChain']
        while i <= num_of_tasks:
            t = 'Task%s' % (i)
            if t not in workload:
                break
            if not 'ConfigCacheID' in workload[t]:
                break
            res.append(workload[t]['ConfigCacheID'])
            i += 1

    return res

def get_expected_events_withdict(dict_from_workload):
    """
    method to takes requests number_of_events/input_ds/block_white_list/run_white_list from rqmgr2 dict
    """

    if 'RequestNumEvents' in dict_from_workload:
        rne = dict_from_workload['RequestNumEvents']
    elif 'RequestSizeEvents' in dict_from_workload:
        rne = dict_from_workload['request']['schema']['RequestSizeEvents']
    elif 'Task1' in dict_from_workload and 'RequestNumEvents' in dict_from_workload['Task1']:
        rne = dict_from_workload['Task1']['RequestNumEvents']
    else:
        rne = None

    if 'FilterEfficiency' in dict_from_workload:
        f = float(dict_from_workload['FilterEfficiency'])
    elif 'Task1' in dict_from_workload and 'FilterEfficiency' in dict_from_workload['Task1']:
        f = float(dict_from_workload['Task1']['FilterEfficiency'])
    else:
        f = 1.

    if 'InputDatasets' in dict_from_workload:
        ###InputDatasetS are a list
        ids = dict_from_workload['InputDatasets']
    elif 'Task1' in dict_from_workload and 'InputDataset' in dict_from_workload['Task1']:
        ###while in taskchain input is a string of Dataset
        ids = [dict_from_workload['Task1']['InputDataset']]
    else:
        ids = []

    if 'BlockWhitelist' in dict_from_workload:
        bwl = dict_from_workload['BlockWhitelist']
    elif 'Task1' in dict_from_workload and 'BlockWhitelist' in dict_from_workload['Task1']:
        bwl = dict_from_workload['Task1']['BlockWhitelist']
    else:
        bwl = []

    if 'RunWhitelist' in dict_from_workload:
        rwl = dict_from_workload['RunWhitelist']
    elif 'Task1' in dict_from_workload and 'RunWhitelist' in dict_from_workload['Task1']:
        rwl = dict_from_workload['Task1']['RunWhitelist']
    else:
        rwl = []

    return get_expected_events_withinput(rne, ids, bwl, rwl, f)

def get_expected_events_withinput(rne, ids, bwl, rwl, filter_eff):
    """
    check request number_of_event was extracted from rqmgr2 dict
    if not calculate evts form: input_ds and run_whilelist & block_white_list through DBS3
    """
    print (rne, ids, bwl, rwl, filter_eff)
    #wrap up
    if rne == 'None' or rne == None or rne == 0:
        s = 0.
        for d in ids:
            if len(rwl):
                print "Doing run selection"
                try:
                    ret = generic_get(dbs3_url + "blocks?dataset=%s" % (d)) #returns blocks names
                    blocks = ret
                except:
                    print d,"does not exist, and therefore we cannot get expected events from it"
                    blocks = None
                if blocks:
                    for run in rwl:
                        print "checking run: %s" % (run)
                        ret = generic_get(dbs3_url + "filesummaries?dataset=%s&run_num=%s" % (d, run)) #returns blocks names
                        data = ret
                        try:
                            s += int(data[0]["num_event"])
                        except:
                            print d, "does not have event for", run
            else:
                print "Doing block selection"
                ret = generic_get(dbs3_url+"blocks?dataset=%s" %(d)) #returns blocks names ????
                blocks = ret
                if len(bwl):
                    ##we have a block white list in input
                    for b in bwl:
                        if not '#' in b: continue
                        for bdbs in filter(lambda bl: bl["block_name"] == b, blocks):
                            block_data = generic_get(dbs3_url + "blocksummaries?block_name=%s" % (bdbs["block_name"].replace("#","%23"))) #encode # to HTML URL
                            s += block_data[0]["num_event"] #because [1] is about replica of block???
                else:
                    for bdbs in blocks:
                        block_data = generic_get(dbs3_url + "blocksummaries?block_name=%s" % (bdbs["block_name"].replace("#","%23"))) #encode # to HTML URL
                        s += block_data[0]["num_event"]
        return s*filter_eff

        ##TO-DO do we need this code? its never executed due to return up top...
        #work from input dbs and block white list
        print "##DEBUG executing a code which is not supposed to run"
        if len(bwl):
            s = 0.
            #print "from block white list"
            for b in bwl:
                if not '#' in b: #a block whitelist needs that
                    continue
                try:
                    ret = generic_get(dbs3_url + "blocksummaries?block_name=%s" % (b.replace("#","%23"))) #encode # to HTML URL
                    data = ret
                    s += data[0]["num_event"]
                except:
                    print b, 'does not have events'
            return s*filter_eff
        else:
            s = 0.
            for d in ids:
                try:
                    ret = generic_get(dbs3_url + "blocksummaries?dataset=%s" % (d))
                    data = ret
                    s += data[0]["num_event"]
                except:
                    print d, "does not have events"
            return s*filter_eff
    else:
        return rne

#-------------------------------------------------------------------------------
## methods to format a date from ReqMngr workload date lien
def timelist_to_str(timelist):
    if len(timelist) == 6:
        (h, m, s) = map(int, timelist)[3:]
    else:
        ##we have Backfill wfs which doesn't have seconds in their time list.
        print "ERROR RquestDate is malformated %s" % (timelist)
        (h, m, s) = timelist[3], timelist[4], 0
    return "%02d%02d%02d" % (h, m, s)

def datelist_to_str(datelist):
    year = str(datelist[0])[2:]
    month = str(datelist[1])
    if len(month) == 1:
        month = "0%s" % month
    day = str(datelist[2])
    if len(day) == 1:
        day = "0%s" % day
    datestr = "%s%s%s" % (year, month, day)
    return datestr

#-------------------------------------------------------------------------------

def get_running_days(raw_date):
    try:
        now = datetime.datetime.now()
        yy = int(raw_date[0:2])
        mm = int(raw_date[2:4])
        dd = int(raw_date[4:6])
        then = datetime.datetime(int("20%s" % yy), mm, dd, 0, 0, 0)
        return int((now-then).days)
    except:
        return -1

#-------------------------------------------------------------------------------

def get_campaign_from_prepid(prepid):
    try:
        return prepid.split("-")[1]
    except:
        return "ByHand"

#-------------------------------------------------------------------------------
@timer
def get_status_nevts_from_dbs(dataset):

    print "You Loose 10CHF antanas"

    undefined = (None, 0, 0)
    debug = False
    if dataset == 'None Yet':
        return undefined
    if dataset == '?':
        return undefined

    total_evts = 0
    total_open = 0

    try:
        ret = generic_get(dbs3_url + "filesummaries?dataset=%s" % (dataset)) #returns blocks names
        if len(ret): ##check if returned data is not empty
            total_evts = int(ret[0]["num_event"])
        print "total_evts_new ", total_evts
    except:
        print "Failed to get total_evts for: %s" % (dataset)
        import traceback
        print traceback.format_exc()
        blocks = []
        return undefined

    try:
        ret = generic_post(dbs3_url+"datasetlist", {"dataset":[dataset], "detail":True})
        result = json.loads(ret) #post method return data as string so we must load it
        if len(result) != 0 :
            status = str(result[0]["dataset_access_type"])
        else:
            status = None
    except:
        print "\n ERROR getting dataset status. return:%s" % (ret)
        raise Exception("  datasetlist POST data:\n%s \n\n results:\n%s" % ({"dataset":[dataset], "detail":True}, ret))

    if not status:
        status = None

    #print (status,total_evts,total_open)
    return (status, total_evts, total_open)

#-------------------------------------------------------------------------------

def get_running_jobs(req):
    key = "Running"
    nrunning = 0
    if req.has_key(key):
        nrunning = req[key]

    return nrunning

#-------------------------------------------------------------------------------

def get_pending_jobs(req):
    key = "Pending"
    bsubmitted = 0
    if req.has_key(key):
        bsubmitted = req[key]
    return bsubmitted

#-------------------------------------------------------------------------------

def get_all_jobs(req):
    key = "running"
    all_jobs = 0
    if req.has_key(key):
        all_jobs = req[key]
    return all_jobs

#-------------------------------------------------------------------------------
def calc_eta(level, running_days):
    flevel = float(level)
    irunning_days = int(running_days)
    if flevel >= 99.99:
        return 0
    elif flevel <= 0.:
        return -1
    #print level
    #print running_days
    total_days = 100.*irunning_days / flevel
    eta = total_days - running_days
    return round(eta + 0.5)

#-------------------------------------------------------------------------------

def getNewDictFromRqmgr2(req_name):
    """
    Method to get workflow dictionary from RequestManager2 site
    """
    res = json.loads(generic_get("%s?name=%s" % (rqmgr2_url, req_name), do_eval=False))
    return res

def getDictFromWorkload(req_name):
    tmp = getNewDictFromRqmgr2(req_name)['result']
    if len(tmp) == 0:
        print "The request: %s is not in RequestManager2. Which is a glitch" % (req_name)
        return {}
    # print json.dumps(tmp[0],indent=4)
    ##do we need a check if the request data is correct? As we query only for single req_name
    return tmp[0]

##more global variables
numberofrequestnameprocessed = 0
countOld = 0

@timer
def parallel_test(arguments, force=False):
    DEBUGME = False
    global countOld
    req, old_useful_info = arguments
    try:
        if DEBUGME: print "+"
        global numberofrequestnameprocessed
        numberofrequestnameprocessed += 1

        pdmv_request_dict={}

        if DEBUGME: print "-"
        ##faking announced status
        if req["request_name"]=='etorassa_EWK-Summer12_DR53X-00089_T1_IT_CNAF_MSS_batch77res_v1__121009_233442_92':
            req["status"] = 'announced'
        if req["request_name"]=='etorassa_JME-Summer12-00060_batch1_v2__120209_133317_6868':
            req["status"] = 'announced'
        if req["request_name"]=='jbadillo_TOP-Summer12-00234_00073_v0__140124_154429_3541':
            req["status"] = 'announced'
        ##faking rejected status
        if req["request_name"]=='spinoso_SUS-Summer12pLHE-00001_4_v1_STEP0ATCERN_130914_172555_5537':
            req["status"] = "rejected"

        if not "status" in req:
            ## this is garbage anyways
            return {}
        req_status = req["status"]

        if req_status in skippable_stati:
            print "Rquest in skippable_stati"
            return None

        # the name
        req_name = req["request_name"]
        pdmv_request_dict["pdmv_request_name"] = req_name

        phedexObj = None
        phedexObjInput = None

        #allowSkippingOnRandom=0.1
        allowSkippingOnRandom = None

        if DEBUGME:
            print "--"
        #transform the workload in a dictionnary for convenience , can also be made an object
        dict_from_workload = {}


        # check if in the old and in complete_stati, just copy and go on
        matching_reqs = filter(lambda oldreq: oldreq['pdmv_request_name'] == req_name, old_useful_info)
        skewed = False
        deadWood = False
        if len(matching_reqs) == 1:

            pdmv_request_dict = matching_reqs[0]
            if pdmv_request_dict['pdmv_completion_eta_in_DAS'] < 0 and req["status"].startswith('running'):
                skewed = True
            if req["status"].startswith('running'):
                skewed = True
            if req["status"] == 'completed':
                skewed = True
            if 'pdmv_expected_events' in pdmv_request_dict and pdmv_request_dict['pdmv_expected_events'] == -1:
                skewed = True
            if pdmv_request_dict['pdmv_status_in_DAS'] == '?':
                skewed = True
            if pdmv_request_dict['pdmv_dataset_name'] == '?':
                skewed = True
            if pdmv_request_dict['pdmv_dataset_name'] == 'None Yet' and req["status"] not in priority_changable_stati:
                skewed = True
            if pdmv_request_dict['pdmv_evts_in_DAS'] < 0:
                skewed = True
            if pdmv_request_dict['pdmv_evts_in_DAS'] == 0 and req["status"] in ['announced']:
                skewed = True
            if pdmv_request_dict['pdmv_status_in_DAS'] in [None, 'PRODUCTION'] and req["status"] in ['announced', 'normal-archived']:
                skewed = True

            if pdmv_request_dict["pdmv_status_from_reqmngr"] != req_status and req_status != "normal-archived":
                print "CHANGE OF STATUS, do something !"
                skewed = True

            if 'pdmv_monitor_time' in pdmv_request_dict:
                up = time.mktime(time.strptime(pdmv_request_dict['pdmv_monitor_time']))
                now = time.mktime(time.gmtime())
                deltaUpdate = (now - up) / (60. * 60. * 24.)
                ##this (with a small number) will create a huge blow to the updating
                if deltaUpdate > 10 and countOld <= 100:
                  print 'too long ago'
                  skewed = True
                  countOld += 1

            if not 'pdmv_monitor_time' in pdmv_request_dict:
                  skewed = True

            ##known bad status
            for bad in []:#'JME-Summer12_DR53X-00098','JME-Summer12_DR53X-00097','vlimant_Run2012']:
                if bad in req_name:
                    skewed = True
                    break

            #add that field after the fact
            if not 'pdmv_running_sites' in pdmv_request_dict:
                pdmv_request_dict['pdmv_running_sites'] = []

            ##needs to be used by user drives the need to T2/T3 field to be up to date
            needsToBeUsed = True
            notNeededByUSers = ['GEN-SIM', 'GEN-RAW']
            for eachTier in notNeededByUSers:
              if pdmv_request_dict["pdmv_dataset_name"].endswith(eachTier):
                needsToBeUsed = False

            if 'pdmv_status_in_DAS' in pdmv_request_dict and not pdmv_request_dict['pdmv_status_in_DAS'] in ['VALID', 'PRODUCTION']:
                needsToBeUsed = False

            if not needsToBeUsed:
                print "not doing phedex call"

            noSites = True
            if 'pdmv_at_T2' in pdmv_request_dict and len(pdmv_request_dict['pdmv_at_T2']):
                noSites = False
            if 'pdmv_at_T3' in pdmv_request_dict and len(pdmv_request_dict['pdmv_at_T3']):
                noSites = False

            if not force:
                noSites = False

            if noSites and needsToBeUsed and (not 'pdmv_at_T2' in pdmv_request_dict or pdmv_request_dict['pdmv_at_T2'] == []):
                if not phedexObj:
                    phedexObj = phedex(pdmv_request_dict["pdmv_dataset_name"])
                pdmv_request_dict['pdmv_at_T2'] = atT2(phedexObj)
            elif not 'pdmv_at_T2' in pdmv_request_dict:
                pdmv_request_dict['pdmv_at_T2'] = []

            if noSites and needsToBeUsed and (not 'pdmv_at_T3' in pdmv_request_dict or pdmv_request_dict['pdmv_at_T3']==[]):
                if not phedexObj:
                    phedexObj = phedex(pdmv_request_dict["pdmv_dataset_name"])
                pdmv_request_dict['pdmv_at_T3'] = atT3(phedexObj)
            elif not 'pdmv_at_T3' in pdmv_request_dict:
                pdmv_request_dict['pdmv_at_T3'] = []


            if 'pdmv_input_dataset' not in pdmv_request_dict:
                if not dict_from_workload:
                    print "##DEBUG## trying to get dict_from_workload"
                    dict_from_workload = getDictFromWorkload(req_name)
                if not dict_from_workload:
                    return {}
                if 'InputDatasets' in dict_from_workload[req_name] and len(dict_from_workload[req_name]['InputDatasets']):
                    pdmv_request_dict['pdmv_input_dataset'] = dict_from_workload[req_name]['InputDatasets'][0]
                else:
                    pdmv_request_dict['pdmv_input_dataset'] = ''


            if not 'pdmv_custodial_sites' in pdmv_request_dict or pdmv_request_dict['pdmv_custodial_sites']==[]:
                if not phedexObj:
                    phedexObj = phedex(pdmv_request_dict["pdmv_dataset_name"])
                pdmv_request_dict['pdmv_custodial_sites'] = custodials(phedexObj)

            if not 'pdmv_open_evts_in_DAS' in pdmv_request_dict:
                pdmv_request_dict['pdmv_open_evts_in_DAS'] = 0

            if 'amaltaro' in req_name and ('type' in req and req['type'] == 'TaskChain'):
                skewed = False

            #needs to be put by hand once in all workflows
            if (pdmv_request_dict['pdmv_status_from_reqmngr'] in complete_stati) and ('pdmv_performance' not in pdmv_request_dict or pdmv_request_dict['pdmv_performance']=={}):
                print "skewed with no perf"
                from Performances import Performances
                pdmv_request_dict['pdmv_performance'] = Performances(req_name)

            if (pdmv_request_dict['pdmv_status_from_reqmngr'] in complete_stati) and not skewed and not force:
                print "** %s is there already" % req_name
                return pdmv_request_dict

            print req_name," is skewed ? ",skewed
            if allowSkippingOnRandom != None and skewed and random.random()> allowSkippingOnRandom and not force:
                  return pdmv_request_dict

        #if needs to be added to the db since it is not present already
        else:
            decide = random.random()
            if allowSkippingOnRandom != None and decide > allowSkippingOnRandom:
                #update only 1 out of 10
                print "Skipping new request",req_name,len(matching_reqs)
                return {}

        if len(matching_reqs)==1:
            print "Updating", req_name
        else:
            print "Processing", req_name

        if not 'pdmv_configs' in pdmv_request_dict or pdmv_request_dict['pdmv_configs'] == []:
            print "##DEBUG## trying to get dict_from_workload2"
            if not dict_from_workload:
                dict_from_workload = getDictFromWorkload(req_name)
            if not dict_from_workload:
                return {}
            pdmv_request_dict['pdmv_configs'] = configsFromWorkload(dict_from_workload[req_name])

        # assign the date
        if not 'pdmv_submission_date' in pdmv_request_dict or not 'pdmv_submission_time' in pdmv_request_dict:
            ##load it on demand only
            print "##DEBUG## trying to get dict_from_workload3"
            if not dict_from_workload:
                dict_from_workload = getDictFromWorkload(req_name)
            if not dict_from_workload:
                return {}

            pdmv_request_dict["pdmv_submission_date"] = datelist_to_str(
                    dict_from_workload[req_name]['RequestDate'])

            pdmv_request_dict["pdmv_submission_time"] = timelist_to_str(
                    dict_from_workload[req_name]['RequestDate'])

            if DEBUGME:
                print "----"
            if  pdmv_request_dict["pdmv_submission_date"][:2] in ["11", "12"]:
                print "Very old request " + req_name
                return {}

        if DEBUGME:
            print "-----"

        # the request itself!
        pdmv_request_dict["pdmv_request"] = req

        # check the status
        req_status = req["status"]
        pdmv_request_dict["pdmv_status_from_reqmngr"] = req_status

        if req_status in priority_changable_stati:
            pdmv_request_dict["pdmv_status"] = "ch_prio"
        else:
            pdmv_request_dict["pdmv_status"] = "fix_prio"

        #request type
        pdmv_request_dict['pdmv_type'] = req["type"]
        if DEBUGME:
            print "------"
        # number of running days
        if pdmv_request_dict["pdmv_status_from_reqmngr"].startswith('running'):
            #set only when the requests is in running mode, not anytime after
            pdmv_request_dict["pdmv_running_days"] = get_running_days(pdmv_request_dict["pdmv_submission_date"])
        elif not "pdmv_running_days" in pdmv_request_dict:
            pdmv_request_dict["pdmv_running_days"] = 0

        if DEBUGME:
            print "-------"

        # get the running jobs
        pdmv_request_dict["pdmv_all_jobs"] = get_all_jobs(req)

        # get the running jobs
        pdmv_request_dict["pdmv_running_jobs"] = get_running_jobs(req)

        # get the jobs pending in the batch systems
        pdmv_request_dict["pdmv_pending_jobs"] = get_pending_jobs(req)

        # get Extras --> slow down---------------
        raw_expected_evts = None
        priority = -1
        # assign the campaign and the prepid guessing from the string, then try to get it from the extras
        if not dict_from_workload:
            print "##DEBUG## trying to get dict_from_workload4"
            dict_from_workload = getDictFromWorkload(req_name)
        if not dict_from_workload:
            return {}

        if ('PrepID' in dict_from_workload[req_name]) and (dict_from_workload[req_name]['PrepID'] != None):
            print "##DEBUG## prepid is not None"
            prep_id = dict_from_workload[req_name]['PrepID']
        else:
            prep_id = 'No-Prepid-Found'
        ##known comuping fck-ups
        if 'RequestName' in dict_from_workload[req_name]:
            __computing_fck_ups = {
                "alahiff_HIG-Summer12DR53X-02171_00353_v0__141218_204531_2518" : "HIG-Summer12DR53X-02171",
                "alahiff_TOP-Summer12DR53X-00276_00354_v0__141218_204512_1227" : "TOP-Summer12DR53X-00276",
                "alahiff_TOP-Summer12DR53X-00275_00355_v0__141218_204521_7100" : "TOP-Summer12DR53X-00275",
                "alahiff_BTV-Phys14DR-00012_00033_v0_castor_141217_165406_5018" : "BTV-Phys14DR-00012",
                "alahiff_B2G-Phys14DR-00058_00084_v0__150105_195309_2573" : "B2G-Phys14DR-00058",
                "alahiff_EXO-Phys14DR-00078_00053_v0__141217_165317_4158" : "EXO-Phys14DR-00078",
                "alahiff_EXO-Phys14DR-00078_00053_v0__141217_165326_9241" : "EXO-Phys14DR-00078",
                "alahiff_BTV-Phys14DR-00011_00033_v0_castor_141217_165355_3898" : "BTV-Phys14DR-00011",
                "alahiff_BTV-Phys14DR-00016_00033_v0_castor_141217_165416_6880" : "BTV-Phys14DR-00016",
                "alahiff_BTV-Phys14DR-00023_00033_v0_castor_141217_165305_1759" : "BTV-Phys14DR-00023",
                "alahiff_HIG-Phys14DR-00042_00080_v0__150105_195258_4618" : "HIG-Phys14DR-00042",
                "jen_a_ACDC_HIG-Phys14DR-00042_00080_v0__150113_211610_5968" : "HIG-Phys14DR-00042",
                "alahiff_BTV-Phys14DR-00023_00033_v0_castor_141217_165437_3766" : "BTV-Phys14DR-00023",
                "alahiff_BTV-Phys14DR-00021_00033_v0_castor_141217_165426_7676" : "BTV-Phys14DR-00021",
                "alahiff_HIG-Phys14DR-00041_00081_v0__150105_195249_1950" : "HIG-Phys14DR-00041",
                "jen_a_ACDC_HIG-Phys14DR-00041_00081_v0__150113_211059_5006" : "HIG-Phys14DR-00041"
            }
            if dict_from_workload[req_name]['RequestName'] in __computing_fck_ups:
                prep_id = __computing_fck_ups[dict_from_workload[req_name]['RequestName']]

        pdmv_request_dict["pdmv_prep_id"] = prep_id
        campaign = get_campaign_from_prepid(prep_id)
        pdmv_request_dict["pdmv_campaign"] = campaign
        req_extras = {}

        # Priority
        retrievePriority = False
        if not 'pdmv_priority' in pdmv_request_dict:
            retrievePriority = True
        if not 'pdmv_present_priority' in pdmv_request_dict:
            retrievePriority = True
        if pdmv_request_dict['pdmv_status_from_reqmngr'] in priority_changable_stati + ['acquired', 'running', 'running-open', 'running-closed']:
            retrievePriority = True

        if retrievePriority:
            print "##DEBUG## trying to get dict_from_workload5"
            if not dict_from_workload:
                dict_from_workload = getDictFromWorkload(req_name)
            if not dict_from_workload:
                return {}
            if 'InitialPriority' in dict_from_workload[req_name]:
                pdmv_request_dict["pdmv_priority"] = dict_from_workload[req_name]['InitialPriority']
            else:
                pdmv_request_dict["pdmv_priority"] = dict_from_workload[req_name]['RequestPriority']

            if DEBUGME:
                print "----------"
            # Present priority
            pdmv_request_dict["pdmv_present_priority"] = dict_from_workload[req_name]['RequestPriority']

        if DEBUGME:
            print "-----------"
        #------------------------------------------

        # Query yet another service to get the das entry of the prepid
        ## try and reduce the number of calls to get_dataset_name url
        dataset_name = "None Yet"
        dataset_list = []
        if 'pdmv_dataset_name' in pdmv_request_dict:
            dataset_name = pdmv_request_dict["pdmv_dataset_name"]
        if 'pdmv_dataset_list' in pdmv_request_dict:
            dataset_list = pdmv_request_dict['pdmv_dataset_list']
        makedsnquery = False
        if (not 'pdmv_dataset_name' in pdmv_request_dict):
            makedsnquery = True
        if 'pdmv_dataset_name' in pdmv_request_dict and (pdmv_request_dict["pdmv_dataset_name"] in ['?','None Yet'] or 'None-' in pdmv_request_dict["pdmv_dataset_name"] or '24Aug2012' in pdmv_request_dict["pdmv_dataset_name"]):
            makedsnquery = True

        if 'pdmv_dataset_list' in pdmv_request_dict and not pdmv_request_dict['pdmv_dataset_list']:
            makedsnquery = True

        if force:
            makedsnquery = True

        if req_status in priority_changable_stati:
            makedsnquery = False

        if req_status == "assigned":
            makedsnquery = True

        if makedsnquery:
            dataset_name, dataset_list = get_dataset_name(
                    dict_from_workload[req_name]['OutputDatasets'])

        pdmv_request_dict["pdmv_dataset_name"] = dataset_name
        pdmv_request_dict["pdmv_dataset_list"] = dataset_list

        deltaUpdate = 0
        ##remove an old field
        if 'pdmv_update_time' in pdmv_request_dict:
            pdmv_request_dict.pop('pdmv_update_time')

        if 'pdmv_monitor_time' in pdmv_request_dict:
            up = time.mktime(time.strptime(pdmv_request_dict['pdmv_monitor_time']))
            now = time.mktime(time.gmtime())
            deltaUpdate = (now-up) / (60. * 60. * 24.)
        else:
            pdmv_request_dict["pdmv_monitor_time"] = time.asctime()

        if deltaUpdate > 2. or DEBUGME or force or pdmv_request_dict['pdmv_status_from_reqmngr'].startswith('running') or skewed:
            #do the expensive procedure rarely or for request which have been update more than 2 days ago
            print "\tSumming for", req_name
            status,evts,openN = get_status_nevts_from_dbs(pdmv_request_dict["pdmv_dataset_name"])
            ## aggregate number of events for all output datasets
            pdmv_request_dict['pdmv_dataset_statuses'] = {}
            for other_ds in pdmv_request_dict['pdmv_dataset_list']:
                if other_ds == pdmv_request_dict["pdmv_dataset_name"]:
                    other_status,other_evts,other_openN = status,evts,openN
                else:
                    other_status,other_evts,other_openN = get_status_nevts_from_dbs( other_ds )
                pdmv_request_dict['pdmv_dataset_statuses'][other_ds] = {
                        'pdmv_status_in_DAS': other_status,
                        'pdmv_evts_in_DAS': other_evts,
                        'pdmv_open_evts_in_DAS': other_openN}
            if status:
                print "\t\tUpdating %s %s %s" % (status, evts, openN)
                pdmv_request_dict["pdmv_status_in_DAS"] = status
                pdmv_request_dict["pdmv_evts_in_DAS"] = evts
                pdmv_request_dict["pdmv_open_evts_in_DAS"] = openN
                ## add this only when the summing is done
            else:
                print "\t\tPassed %s %s %s" % (status, evts, openN)
                pass
            pdmv_request_dict["pdmv_monitor_time"] = time.asctime()
        else:
            print "\tSkipping summing for",req_name
            ## move the next line out to go over the "pass" above

        if not "pdmv_status_in_DAS" in pdmv_request_dict:
            pdmv_request_dict["pdmv_status_in_DAS"] = None
        if not "pdmv_evts_in_DAS" in pdmv_request_dict:
            pdmv_request_dict["pdmv_evts_in_DAS"] = 0
        if not "pdmv_open_evts_in_DAS" in pdmv_request_dict:
            pdmv_request_dict["pdmv_open_evts_in_DAS"] = 0

        if not 'pdmv_expected_events' in pdmv_request_dict:
            pdmv_request_dict['pdmv_expected_events'] = -1

        if not 'pdmv_expected_events_per_ds' in pdmv_request_dict:
            pdmv_request_dict['pdmv_expected_events_per_ds'] = {}

        ## why having "expected events" not calculated if the output dataset has no status yet ?
        #to prevent stupid update from garbage requests announced or skewed but enrecoverable
        if pdmv_request_dict["pdmv_expected_events"] < 0 or (skewed and pdmv_request_dict['pdmv_status_in_DAS'] and not pdmv_request_dict['pdmv_status_in_DAS'] in ['DELETED', 'DEPRECATED'] and pdmv_request_dict["pdmv_expected_events"] < 0) or force:
            print "Getting expected events"
            print "##DEBUG## trying to get dict_from_workload5.5"
            if not dict_from_workload:
                dict_from_workload = getDictFromWorkload(req_name)
            if not dict_from_workload:
                return {}

            raw_expected_evts = get_expected_events_withdict(dict_from_workload[req_name])
            if DEBUGME:
                print "--------"

            # get the expected number of evts
            expected_evts =- 1
            if raw_expected_evts == None or raw_expected_evts == 'None':
                expected_evts =- 1
            else:
                expected_evts = int(raw_expected_evts)
            if DEBUGME:
                print "---------"
            pdmv_request_dict["pdmv_expected_events"] = expected_evts
            ## get an expected per output dataset if possible
            ##Reqmgr2 doesn't support this so we set to empty dict for all wfs
            pdmv_request_dict['pdmv_expected_events_per_ds'] = {}

        completion = 0
        if  pdmv_request_dict["pdmv_expected_events"] > 0.:
            completion = 100*(float(pdmv_request_dict["pdmv_evts_in_DAS"]) + float(pdmv_request_dict["pdmv_open_evts_in_DAS"]))/pdmv_request_dict["pdmv_expected_events"]

        pdmv_request_dict["pdmv_completion_in_DAS"] = float("%2.2f" % completion)
        if DEBUGME:
            print "----------"
        pdmv_request_dict["pdmv_completion_eta_in_DAS"] = calc_eta(pdmv_request_dict["pdmv_completion_in_DAS"], pdmv_request_dict["pdmv_running_days"])

        if not 'pdmv_running_sites' in pdmv_request_dict:
            pdmv_request_dict['pdmv_running_sites'] = []

        #find open blocks for running at sites
        if pdmv_request_dict['pdmv_status_from_reqmngr'].startswith('running'):
            if not phedexObj:
                phedexObj = phedex(pdmv_request_dict["pdmv_dataset_name"])
            pdmv_request_dict['pdmv_running_sites'] = runningSites(phedexObj)

        if not 'pdmv_assigned_sites' in pdmv_request_dict:
            pdmv_request_dict['pdmv_assigned_sites'] = []

        if not 'pdmv_custodial_sites' in pdmv_request_dict:
            pdmv_request_dict['pdmv_custodial_sites'] = []

        if (pdmv_request_dict['pdmv_assigned_sites'] == []
                or pdmv_request_dict['pdmv_custodial_sites'] == []):

            print "##DEBUG## trying to get dict_from_workload6"
            if not dict_from_workload:
                dict_from_workload = getDictFromWorkload(req_name)
            if not dict_from_workload:
                return {}
            #find out the keys that has constraints
            if 'SiteWhitelist' in dict_from_workload[req_name]:
                sites = set(dict_from_workload[req_name]['SiteWhitelist'])
            else:
                sites = []

            if pdmv_request_dict['pdmv_status_in_DAS']:
                if not phedexObj:
                    phedexObj = phedex(pdmv_request_dict["pdmv_dataset_name"])
                    pdmv_request_dict['pdmv_custodial_sites'] = custodials(phedexObj)
                    ##TO-DO works only for 1st entry of site?
                    if pdmv_request_dict['pdmv_custodial_sites'] == []:
                        pdmv_request_dict['pdmv_custodial_sites'] = filter(
                                lambda s : s.startswith('T1_'), sites)

            pdmv_request_dict['pdmv_assigned_sites'] = list(sites)

        needsToBeUsed = True
        notNeededByUSers = ['GEN-SIM', 'GEN-RAW']
        for eachTier in notNeededByUSers:
            if pdmv_request_dict["pdmv_dataset_name"].endswith(eachTier):
                needsToBeUsed = False

        noSites = True
        if 'pdmv_at_T2' in pdmv_request_dict and len(pdmv_request_dict['pdmv_at_T2']):
            noSites = False
        if 'pdmv_at_T3' in pdmv_request_dict and len(pdmv_request_dict['pdmv_at_T3']):
            noSites = False

        if noSites and needsToBeUsed and (not 'pdmv_at_T2' in pdmv_request_dict or pdmv_request_dict['pdmv_at_T2']==[]):
            if not phedexObj:
                phedexObj = phedex(pdmv_request_dict["pdmv_dataset_name"])

            pdmv_request_dict['pdmv_at_T2'] = atT2(phedexObj)
        elif not 'pdmv_at_T2' in pdmv_request_dict:
            pdmv_request_dict['pdmv_at_T2'] = []

        if noSites and needsToBeUsed and (not 'pdmv_at_T3' in pdmv_request_dict or pdmv_request_dict['pdmv_at_T3']==[]):
            if not phedexObj:
                phedexObj = phedex(pdmv_request_dict["pdmv_dataset_name"])

            pdmv_request_dict['pdmv_at_T3'] = atT3(phedexObj)
        elif not 'pdmv_at_T3' in pdmv_request_dict:
            pdmv_request_dict['pdmv_at_T3'] = []

        if (pdmv_request_dict['pdmv_status_from_reqmngr'] in complete_stati) and ('pdmv_performance' not in pdmv_request_dict or pdmv_request_dict['pdmv_performance']=={}):
            print "updating for perf"
            from Performances import Performances
            pdmv_request_dict['pdmv_performance'] = Performances(req_name)

        print "#numebr of processed: %s" % (numberofrequestnameprocessed)
        return pdmv_request_dict

    except:
        import traceback
        ##something failed lets produce traceback file for future investigations...
        trf = 'traceback/traceback_%s.txt' % (req["request_name"])
        print req["request_name"],"IS A DEAD FAILING REQUEST"
        print trf
        tr = open(trf, 'w')
        tr.write("dict_from_workload\n")
        tr.write(json.dumps(dict_from_workload))
        tr.write("\n")
        tr.write("pdmv_request_dict\n")
        tr.write(json.dumps(pdmv_request_dict))
        tr.write("\n")
        tr.write("req:\n")
        tr.write(json.dumps(req))
        tr.write(traceback.format_exc())
        tr.close()

        return {}
