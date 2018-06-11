#!/usr/bin/env python

import json
import pprint
import multiprocessing
import itertools
import copy
import optparse
import time
import traceback
import os
import sys
import logging

from couchDB import Interface


FORCE = False
logger = None

def insertAll(req_list,docs,pattern=None,limit=None):
    newentries=0
    for req in req_list:
        docid=req["request_name"]
        ## not already present
        if docid in docs:            continue
        ## not broken
        if not "status" in req:            continue
        ## not aborted or rejected
        if req["status"] in ['aborted','rejected']:        continue
        ## not according to pattern search
        if pattern and not pattern in docid:            continue
        ## to limit things a bit
        if limit and newentries>=limit:    break

        #print docid,"is not in the stats cache yet"
        if insertOne(req):
            newentries+=1
    print newentries,"inserted"


def insertOne(req):
    ##req is from wmstats
    global statsCouch
    from statsMonitoring import parallel_test
    #pprint.pprint(req)
    updatedDoc=parallel_test( [req,[]] )
    docid=req["request_name"]
    if updatedDoc==None or not len(updatedDoc):
        print "failed to get anything for",docid
        return False
    updatedDoc['_id'] = docid
    statsCouch.create_file(json.dumps(updatedDoc))
    #pprint.pprint(updatedDoc)
    return docid


def worthTheUpdate(new, old):
    # make a tighter selection on when to update in couchDB to not overblow it with multiple updates per 4 hours
    global FORCE
    if FORCE:
        return True

    if old['pdmv_evts_in_DAS'] != new['pdmv_evts_in_DAS']:
        return True
    if old['pdmv_status_in_DAS'] != new['pdmv_status_in_DAS']:
        return True
    if old['pdmv_status_from_reqmngr'] != new['pdmv_status_from_reqmngr']:
        return True

    if old != new:
        # what about monitor time ???? that is different ?
        if set(old.keys()) != set(new.keys()):
            # addign a new parameter
            return True

        if old['pdmv_dataset_statuses'] != new['pdmv_dataset_statuses']:
            return True

        # otherwise do not update, even with minor changes
        print "minor changes to", new['pdmv_request_name'], new['pdmv_evts_in_DAS'], "is more than", old['pdmv_evts_in_DAS']
        print old['pdmv_status_from_reqmngr'], new['pdmv_status_from_reqmngr']
        return False
    else:
        return False


def compare_dictionaries(dict1, dict2):
    if dict1 is None or dict2 is None:
        return False

    if type(dict1) is not dict or type(dict2) is not dict:
        return False

    shared_keys = set(dict1.keys()) & set(dict2.keys())
    if not (len(shared_keys) == len(dict1.keys()) and len(shared_keys) == len(dict2.keys())):
        return False

    try:
        dicts_are_equal = True
        for key in dict1.keys():
            if type(dict1[key]) is dict:
                dicts_are_equal = dicts_are_equal and compare_dictionaries(dict1[key], dict2[key])
            else:
                dicts_are_equal = dicts_are_equal and (dict1[key] == dict2[key])

            if not dicts_are_equal:
                return False
        return dicts_are_equal
    except:
        return False


def updateOne(docid, match_req_list):
    if "dmason" in docid:
        logger.info("Its a dmason request: %s" % (docid))
        return False
    # if "anorkus" not in docid:
    #     print "Not anorkus request: %s" % (docid)
    #     return False
    global statsCouch
    try:
        thisDoc = statsCouch.get_file_info(docid)
    except Exception as ex:
        logger.error("There was an access crash with %s. Exception %s" % (docid, str(ex)))
        return False

    updatedDoc = copy.deepcopy(thisDoc)
    if not len(match_req_list):
        # when there is a fake requests in stats.
        if docid.startswith('fake_'):
            match_req_list = [{"request_name": docid, "status": "announced", "type": "ReDigi"}]
            logger.warning("FAKE %s" % (docid))
        return False

    if len(match_req_list) > 1:
        logger.warning("More than one! %s" % (docid))

    req = match_req_list[0]
    from statsMonitoring import parallel_test
    global FORCE
    # print "##DEBUG## sending a request for parallel_test.\nreq:%s\nupdatedDoc:%s" % (req, updatedDoc)
    updatedDoc = parallel_test([req, [updatedDoc]], force=FORCE)
    if updatedDoc == {}:
        logger.error("Updating %s returned an empty dict" % (docid))
        return False

    if updatedDoc is None:
        logger.info("Deleting %s" % (docid))
        pprint.pprint(thisDoc)
        statsCouch.delete_file_info(docid, thisDoc['_rev'])
        return False

    if worthTheUpdate(updatedDoc, thisDoc):
        to_get = ['pdmv_monitor_time',
                  'pdmv_evts_in_DAS',
                  'pdmv_open_evts_in_DAS',
                  'pdmv_dataset_statuses']
        if 'pdmv_monitor_history' not in updatedDoc:
            # do the migration
            thisDoc_bis = statsCouch.get_file_info_withrev(docid)
            revs = thisDoc_bis['_revs_info']
            history = []
            for rev in revs:
                try:
                    nextOne = statsCouch.get_file_info_rev(docid, rev['rev'])
                except:
                    continue

                history.append({})
                for g in to_get:
                    if g not in nextOne:
                        continue

                    history[-1][g] = copy.deepcopy(nextOne[g])

            updatedDoc['pdmv_monitor_history'] = history

        if 'pdmv_monitor_history' in updatedDoc:
            rev = {}
            for g in to_get:
                if g not in updatedDoc:
                    continue

                rev[g] = copy.deepcopy(updatedDoc[g])

            old_history = copy.deepcopy(updatedDoc['pdmv_monitor_history'][0])
            new_history = copy.deepcopy(rev)
            del old_history["pdmv_monitor_time"]  # compare history without monitor time
            del new_history["pdmv_monitor_time"]
            if not compare_dictionaries(old_history, new_history):  # it is worth to fill history
                updatedDoc['pdmv_monitor_history'].insert(0, rev)

        try:
            statsCouch.update_file(docid, json.dumps(updatedDoc))
            logger.info("Something has changed %s" % (docid))
            return docid
        except Exception as ex:
            logger.error("Failed to update %s. Exception %s" % (docid, str(ex)))
            return False
    else:
        logger.info("%s not worth the update (nothing changed)" % (docid))
        return False


def updateOneIt(key_value_pair):
    docid = key_value_pair[0]
    request_dict = key_value_pair[1]
    return updateOne(docid, request_dict)


def dumpSome(docids,limit):
    dump=[]
    for docid in docids:
        if limit and len(dump)>= limit: break
        try:
            a_doc = statsCouch.get_file_info( docid)
            dump.append( a_doc )
            dump[-1].pop('_id')
            dump[-1].pop('_rev')
        except:
            pass
    return dump

docs=[]
statsCouch=None

def main():
    global docs,FORCE,statsCouch
    usage= "Usage:\n %prog options"
    parser = optparse.OptionParser(usage)
    parser.add_option("--search",
                      default=None
                      )
    parser.add_option("--test",
                      default=False,
                      action='store_true'
                      )
    parser.add_option("--force",
                      default=False,
                      action='store_true'
                      )
    parser.add_option("--inspect",
                      default=False,
                      action='store_true'
                      )
    parser.add_option("--do",
                      choices=['update','insert','kill','list'],
                      )
    parser.add_option("--db",
                      default="http://localhost")
    ### Due to migration for ReqMgr2
    ##REMOVE BEFORE PUSH TO PROD
    # parser.add_option("--db",
    #                   default="http://anorkus-test4.cern.ch")
    parser.add_option("--mcm",
                      default=False,
                      help="drives the update from submitted requests in McM",
                      action="store_true")
    parser.add_option("--nowmstats",
                      default=False,
                      help="Goes back to query the full content of request manager",
                      action="store_true")
    parser.add_option("--check",
                      default=False,
                      help="Prevent two from running at the same time",
                      action="store_true")

    options,args=parser.parse_args()

    return main_do( options )


def main_do(options):
    logger.info("Running main")
    if options.check:
        logger.info('Check')
        # we check if this script is already running with same parameters#
        checks = ['ps -f -u $USER']
        for arg in sys.argv[1:]:
            checks.append('grep "%s"' % (arg.split('/')[-1].replace('--', '')))
        checks.append('grep -v grep')
        check = filter(None, os.popen("|".join(checks)).read().split('\n'))
        if len(check) != 1:
            logger.error("Already running with that exact setting")
            logger.info(check)
            sys.exit(1)
        else:
            logger.info("ok to operate")

    start_time = time.asctime()
    global statsCouch, docs, FORCE
    # interface to the couchDB
    statsCouch = Interface(options.db + ':5984/stats')

    # get from stats couch the list of requests
    view = 'yearAgo' if options.do == 'update' else 'all'
    # in case we want to force update even older workflows
    if options.force:
        view = 'all'

    logger.info("Getting all stats ...")
    allDocs = statsCouch.get_view(view)
    docs = set([doc['id'] for doc in allDocs['rows']])
    # remove the _design/stats
    if view == 'all':
        docs = set(filter(lambda doc: not doc.startswith('_'), docs))

    logger.info("... done")

    nproc = 4
    limit = None
    if options.test:
        limit = 10

    if options.do == 'insert':
        logger.info('do = insert')
        # get from wm couch
        from statsMonitoring import parallel_test, get_requests_list
        logger.info("Getting all req ...")
        req_list = get_requests_list()
        logger.info("... done")

        # insert new requests, not already in stats couch into stats couch
        # insertAll(req_list,docs,options.search,limit)

        logger.info('Will filter')
        if options.search:
            req_list = filter(lambda req: options.search in req["request_name"], req_list)
            logger.info('%d requests after search' % (len(req_list)))

        # print "req_list: " % (req_list)
        # skip malformated ones
        req_list = filter(lambda req: "status" in req, req_list)
        logger.info('%d requests after skipping malformed' % (len(req_list)))

        # take only the ones not already in there
        req_list = filter(lambda req: req["request_name"] not in docs, req_list)
        logger.info('%d after taking only those not already in there' % (len(req_list)))

        # skip trying to insert aborted and rejected or failed
        # req_list = filter( lambda req : not req["status"] in ['aborted','rejected','failed','aborted-archived','rejected-archived','failed-archived'], req_list )
        req_list = filter(lambda req: not req["status"] in ['aborted', 'rejected', 'failed', None], req_list)
        logger.info('%d after skipping aborted, rejected, failed and None' % (len(req_list)))

        # do not update TaskChain request statuses
        # req_list = filter( lambda req : 'type' in req and req['type']!='TaskChain', req_list)
        # logger.info('Requests %d' % (len(req_list)))

        if limit:
            req_list = req_list[0:limit]
            logger.info('%d after limiting' % (len(req_list)))

        logger.info('Dispatching %d requests to %d processes' % (len(req_list), nproc))
        pool = multiprocessing.Pool(nproc)
        results = pool.map(insertOne, req_list)
        logger.info('End dispatching')

        results = filter(lambda item: item is not False, results)
        logger.info('%d inserted' % (len(results)))
        logger.info(str(results))
        """
        showme=''
        for r in results:
            showme+='\t'+r+'\n'
        print showme
        """
    elif options.do == 'kill' or options.do == 'list':
        logger.info('do = kill OR do = list')
        # get from wm couch
        from statsMonitoring import parallel_test, get_requests_list
        logger.info("Getting all req ...")
        req_list = get_requests_list()
        logger.info("... done")

        removed = []
        if options.search:
            req_list = filter(lambda req: options.search in req["request_name"], req_list)
            for r in req_list:
                logger.info("Found %s in status %s?" % (r['request_name'], (r['status'] if 'status' in r else 'undef')))
                if options.do == 'kill':
                    # print "killing",r['request_name'],"in status",(r['status'] if 'status' in r else 'undef'),"?"
                    docid = r['request_name']
                    if docid in docs and docid not in removed:
                        thisDoc = statsCouch.get_file_info(docid)
                        logger.info("Removing record for docid %s" % (docid))
                        statsCouch.delete_file_info(docid, thisDoc['_rev'])
                        removed.append(docid)
                    else:
                        logger.info("Nothing to kill")

    elif options.do == 'update':
        logger.info('do = update')
        __newest = True
        if options.search:
            __newest = False
        # get from wm couch
        from statsMonitoring import get_requests_list
        logger.info("Getting all req ...")
        req_list = get_requests_list(not_in_wmstats=options.nowmstats, newest=__newest)
        logger.info("... done")

        cookie_path = '/home/pdmvserv/private/prod_cookie.txt'
        if options.mcm:
            sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')
            from rest import restful
            mcm = restful(dev=False, cookie=cookie_path)
            rs = mcm.getA('requests', query='status=submitted')
            rids = map(lambda d: d['prepid'], rs)

            logger.info("Got %d to update from mcm" % (len(rids)))
            # print len(docs),len(req_list)
            # print map( lambda docid : any( map(lambda rid : rid in doc, rids)), docs)
            docs = filter(lambda docid: any(map(lambda rid: rid in docid, rids)), docs)
            if len(docs):
                # req_list = filter(lambda req: any(map(lambda rid: rid in req["request_name"], rids)), req_list)
                req_list = filter(lambda req: req['request_name'] in docs, req_list)

        if options.search:
            if options.force:
                FORCE = True
            docs = filter(lambda docid: options.search in docid, docs)
            if len(docs):
                # req_list = filter(lambda req: options.search in req["request_name"], req_list)
                req_list = filter(lambda req: req['request_name'] in docs, req_list)
                if len(req_list):
                    pprint.pprint(req_list)

        if limit:
            req_list = req_list[0:limit]

        request_dict = {}
        for request in req_list:
            if request['request_name'] in request_dict:
                request_dict[request['request_name']].append(request)
                logger.info('APPEND! %s' % (request['request_name']))
            else:
                request_dict[request['request_name']] = [request]

        logger.info("Dispaching %d requests to %d processes..." % (len(request_dict), nproc))
        pool = multiprocessing.Pool(nproc)
        results = pool.map(updateOneIt, request_dict.iteritems())

        logger.info("End dispatching")

        if options.search:
            dump = dumpSome(docs, limit)
            logger.info("Result from update with search")
            pprint.pprint(dump)

        results = filter(lambda item: item is not False, results)
        logger.info('%d updated' % (len(results)))
        logger.info(str(results))

        print "\n\n"
        for r in results:
            try:
                withRevisions = statsCouch.get_file_info_withrev(r)
                # we shouldnt trigger mcm for ReRecos or Relvals which doesnt exist there
                if any(el in withRevisions['pdmv_prep_id'].lower() for el in ['relval', 'rereco']):
                    logger.info("NOT bothering McM for rereco or relval")
                    continue
                # notify McM for update !!
                if (withRevisions['pdmv_prep_id'].strip() not in ['No-Prepid-Found', '', 'None']) and options.inspect and '_' not in withRevisions['pdmv_prep_id']:
                    logger.info("Notifying McM for update")
                    inspect = 'curl -s -k -L --cookie %s https://cms-pdmv.cern.ch/mcm/restapi/requests/inspect/%s' % (cookie_path, withRevisions['pdmv_prep_id'])
                    # TO-DO change for reqgmr2 migration
                    # inspect = 'curl -s -k --cookie ~/private/dev-cookie.txt https://cms-pdmv-dev.cern.ch/mcm/restapi/requests/inspect/%s' % withRevisions['pdmv_prep_id']
                    os.system(inspect)
                # he we should trigger McM update if request is in done.
                # because inspection on done doesn't exists.
                if (withRevisions['pdmv_type'] != 'Resubmission' and
                    withRevisions['pdmv_prep_id'].strip() not in ['No-Prepid-Found', '', 'None', '_'] and
                    withRevisions['pdmv_status_from_reqmngr'] == "normal-archived"):
                    ## we should trigger this only if events_in_das was updated for done
                    update_comm = 'curl -s -k -L --cookie %s https://cms-pdmv.cern.ch/mcm/restapi/requests/update_stats/%s/no_refresh' % (cookie_path, withRevisions['pdmv_prep_id'])
                    #update_comm = 'curl -s -k --cookie ~/private/dev-cookie.txt https://cms-pdmv-dev.cern.ch/mcm/restapi/requests/update_stats/%s/no_refresh' % withRevisions['pdmv_prep_id']
                    logger.info("Triggering McM completed_evts syncing for a done request %s" % (withRevisions['pdmv_prep_id']))

                    os.system(update_comm)
            except:
                print "failed to update growth for", r
                print traceback.format_exc()

        print "\n\n"
        # set in the log file
        # serves as forceupdated !
        logger.info("start time: %s" % str(start_time))
        logger.info("logging updating time: %s" % str(time.asctime()))
        log_file = open('stats.log', 'a')
        log_file.write(time.asctime() + '\n')
        log_file.close()


if __name__ == "__main__":
    PROFILE = False
    FORMAT = '[%(asctime)s][%(filename)s:%(lineno)d][%(levelname)s] %(message)s'
    logging.basicConfig(stream=sys.stdout, format=FORMAT, level=logging.INFO)
    global logger
    logger = logging.getLogger()
    if PROFILE:
        import cProfile
        cProfile.run('main()')
    else:
        main()
