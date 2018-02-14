import cherrypy
import json
import urllib2
import subprocess
import sys
from internals.display import HomePage, Initializer, ListOfSimulations
from internals.rest import RestIndex, GetOne, UpdateOne, ProducesDN


# Initialisation , first define the last heart beat to not get and error during the processing of the JSON file
# Next initialization of the counter that will make beating the heart of the application
# Next we create list for the treatment of request and other.
# List Of Attributs will give us the ability to get a lot of things in our code.


@cherrypy.expose
def getAllSimulations():
    data = []
    for elem in ListOfSimulations:
        data.append(elem.getsim())
    return json.dumps(data)


@cherrypy.expose
def manualUpdate():
    Initializer().Actualization()
    return "Updated page cache"


@cherrypy.expose
def getAllDocs():
    f = urllib2.urlopen('http://localhost:5984/stats/_all_docs')
    data = f.read()
    return data


print "### Checking if couchdb-lucene is runing ###"
proc = subprocess.Popen("curl localhost:5985 -s", stdout=subprocess.PIPE, shell=True)
output = proc.communicate()[0]
try:
    json.loads(output)
except:
    print "couchdb-lucene is not runing! Please run:\n sudo nohup /bin/run &\n in couchdb-lucene directory"
    sys.exit(1)

root = HomePage()
root.simulation_list = getAllSimulations
root.update_all = manualUpdate
root.Db_all = getAllDocs
root.restapi = RestIndex()
root.restapi.get_one = GetOne()
root.restapi.update = UpdateOne()
root.restapi.produces = ProducesDN()
# Initializer().Actualization()
cherrypy.quickstart(root, config='prod.conf')
