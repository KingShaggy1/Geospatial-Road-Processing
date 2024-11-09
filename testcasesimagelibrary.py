import getopt, sys
from multiprocessing import Process, Queue, Lock, current_process, freeze_support, get_context
import queue # imported for using queue.Empty exception
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from elasticsearch.helpers import scan
from shapely import speedups

from factstore import FactStore
from processroad import ProcessRoad
import time
from datetime import datetime
from pprint import pprint

class TestCasesImageLibrary:

    def __init__(self, esImageLibrary, esImageLibraryPort, esAnalytics, esAnalyticsPort):
        self.esImageLibrary = esImageLibrary
        self.esImageLibraryPort = esImageLibraryPort
        self.esAnalytics = esAnalytics
        self.esAnalyticsPort = esAnalyticsPort
        self.factSource = None
        self.ROAD_INDEX = ""
        self.geo_coverage_index = ''
        #self.esroadfield = 'roadId'
        #self.DEFAULT_DURATION = 10 * 1000
        self.imgClient = None
        self.rec_count = 0
        self.esClient = None
        self.processcount = 4       #Specify the number of processes to use
        self.batch_size = 8         #Batch size to specify queue size

    def setup(self):
        print('Connecting to ES ...')

        try:
            self.factSource = FactStore(self.esAnalytics, self.esAnalyticsPort)
            self.factSource.start()
        except:
            print('Error connecting to ES...', sys.exc_info())

        print('Test Setup completed')

    def roadHasBeenProcessed(self, linear_id):
        return len(self.factSource.search(self.geo_coverage_index, self.esroadfield, linear_id)) > 0

    def setprocessroadargs(self, func, linearid):

        road_results = self.esClient.search(index=self.ROAD_INDEX, body={"query": {"match":{"LINEARID": linearid}}, "size": 10})

        return linearid, road_results['hits']['hits']

    def callprocessroad(self, task_queue, done_queue, lock):
        while True:
            #print(input)
            #for func, args in iter(input.get, 'STOP'):
            #    rd_linearid, hits = self.setprocessroadargs(func, args)

            try:
                prev_hitsource_map = None

                task_linearid = task_queue.get_nowait()
                road_results = self.esClient.search(index=self.ROAD_INDEX, body={"query": {"match": {"LINEARID": task_linearid}}, "size": 10})

                for hit in road_results['hits']['hits']:
                    hitsource_map = hit.get('_source')

                    if hitsource_map == prev_hitsource_map:
                        break

                    else:
                        # Instantiate class.
                        pr = ProcessRoad()

                        self.rec_count, cmsg = pr.startprocessroad(self.rec_count, self.esClient, self.imgClient, self.factSource, task_linearid, hitsource_map)

                    # Implement lock so that print computed road process info is not mixed up with other processes
                    with lock:

                        #print(cmsg)
                        pprint(cmsg)
                        #sys.stdout.write(cmsg)

                        done_queue.put(self.rec_count)
                        time.sleep(.030)
                        #time.sleep(.300) #Wait for 300 milliseconds #needed some delay or ran too fast and didn't mix output.

                        prev_hitsource_map = hitsource_map
            except queue.Empty:
                break
            except:
                print('Error: ', sys.exc_info())

    def callprocessroad2(self, input, output, lock):
        print(input)
        for func, args in iter(input.get, 'STOP'):
            rd_linearid, hits = self.setprocessroadargs(func, args)

            try:
                prev_hitsource_map = None

                for hit in hits:
                    hitsource_map = hit.get('_source')

                    if hitsource_map == prev_hitsource_map:
                        break

                    else:
                        # Instantiate class.
                        pr = ProcessRoad()

                        self.rec_count, cmsg = pr.startprocessroad(self.rec_count, self.esClient, self.imgClient, self.factSource, rd_linearid, hitsource_map)

                    # Implement lock so that print computed road process info is not mixed up with other processes
                    with lock:

                        print(cmsg)
                        
                        output.put(self.rec_count)
                        time.sleep(.030)
                        #time.sleep(.300) #Wait for 300 milliseconds #needed some delay or ran too fast and didn't mix output.

                        prev_hitsource_map = hitsource_map
            except:
                print('Error: ', sys.exc_info())

    def progressbar(self, count, total, suffix=''):
        bar_len = 30
        filled_len = int(round(bar_len * count / float(total)))

        #percents = round(100.0 * count / float(total), 1)
        percents = round(100 * count / float(total))
        bar = '*' * filled_len + '-' * (bar_len - filled_len)

        sys.stdout.write('[%s] %s%s %s\r' % (bar, percents, '%', suffix))
        sys.stdout.flush()

    def return_processedroads(self):

        processedroads = {}

        qu =  {"size" : 100000, "_source" : {"includes" : ["roadId", "RoadName"],"excludes" : [ ]}}
        #qu =  {"size" : 100000, "_source" : {"includes" : ["roadId"],"excludes" : [ ]}}
        #doc2 =  {"size" : 100000, "_source" : {"includes" : ["roadId"],"excludes" : [ ]},"sort" : [{"RoadName" : {"order" : "asc"}}]}

        resProcessedRds = helpers.scan(client=self.esClient, query=qu, scroll='50m', size=10000, index=self.geo_coverage_index, clear_scroll=True)
        #resProcessedRds = helpers.scan(client=self.esClient, query=qu, size=10000, scroll='5m', request_timeout=10000, index=self.geo_coverage_index, clear_scroll=True)

        for data in resProcessedRds:
            processedroads.update({data['_source']['roadId'].strip(): data['_source']['RoadName']})
            #processedroads.append(data['_source']['roadId'].strip())

        return processedroads

    def testComputeImageCoverage(self):

        freeze_support()
        lock = Lock()
        speedups.enable()

        try:

            self.esClient = self.factSource.getES()

            print('Connecting to ImageLibrary ES Datasource')

            self.imgClient = Elasticsearch(self.esImageLibrary, port=self.esImageLibraryPort, timeout=600, connection_class=RequestsHttpConnection, use_ssl=True, max_retries=10, retry_on_timeout=True)

            print('Connection established for ImageLibrary ES')

            rq = {"size" : 0,"_source" : False,"stored_fields" : "_none_","track_total_hits" : True}
            #rq = {"track_total_hits": True, 'query': {'match_all': {}}}    #This works too.
        
            rdcount = self.esClient.search(index=self.ROAD_INDEX, body = rq, size=0)['hits']['total']['value']

            print('Checking already processed roads ...')
            #Getting already processed roads
            processedroads = self.return_processedroads()
            print(len(processedroads), 'roads already processed')

            linearids = []

            print("Loading roads from es ...")
            
            #starting load at 0%
            self.progressbar(len(linearids), rdcount, "[{} of {} total roads loaded for processing]".format(len(linearids), rdcount))

            doc = {"size": 100000, "_source": {"includes": ["LINEARID", "FULLNAME"], "excludes": []},"sort": [{"FULLNAME": {"order": "asc"}}]}
            #doc = {"size" : 100000, "_source" : {"includes" : ["LINEARID"],"excludes" : [ ]},"sort" : [{"FULLNAME" : {"order" : "asc"}}]}

            #doc = {'size' : 100000, "query":{"match_all":{"boost":1.0}},"sort":[{"FULLNAME":{"order":"asc"}}]}     #Main one for all roads #This was very slow.
            #doc = {"query":{"match":{"LINEARID":"1103764653091"}},"sort":[{"FULLNAME":{"order":"asc"}}]}     #Getting just one road


            res = helpers.scan(client=self.esClient, query=doc, size=10000, scroll='50m', index=self.ROAD_INDEX) #preserve_order expensive operation
            #res = helpers.scan(client=self.esClient, query=doc, size=10000, scroll='50m', index=self.ROAD_INDEX, preserve_order=True)
            #Try to get one or two roads only above for testing. 
            
            all_prisecroads = {}    #Only unique roads.
            for data in res:
                #print(data)
                all_prisecroads.update({data['_source']['LINEARID'].strip(): data.get('_source').get('FULLNAME') or '(Unknown Name)'})

                #if data['_source']['LINEARID'].strip() not in processedroads:
                    #linearids.append(data['_source']['LINEARID']) #TODO road type to list/sets to distinguish if primary/secondary/...

                #Display loaded per size
                self.progressbar(len(all_prisecroads), rdcount, "[{} of {} total roads loaded for processing]".format(len(all_prisecroads), rdcount))
            
            #set1 = set(all_prisecroads.items())
            #set2 = set(processedroads.items())
            #Get the unproceessed roads
            diff = set(all_prisecroads.items()) ^ set(processedroads.items())
            #sort by road name.
            sort_diff = sorted(diff, key=lambda x: x[1])
            #get the Linearids
            linearids = [i[0] for i in sort_diff]

            #Check if linearids[i] is not null.

            #Get already processed Roads.

            #processedroads = self.return_processedroads()

            #print('\nRemoving processed roads from list ...')
            #linearids = list(set(linearids) - set(processedroads))
            #linearids = list(set(linearids).difference(set(processedroads)))

            #doc2 =  {"size" : 100000, "_source" : {"includes" : ["roadId"],"excludes" : [ ]},"sort" : [{"RoadName" : {"order" : "asc"}}]}
            #resProcessedRds = helpers.scan(client=self.esClient, query=doc2, size=10000, scroll='5m', index='emmanuel_test_index', preserve_order=True)

            

            #Remove duplicates from list
            linearids = list(dict.fromkeys(linearids))

            print('\n**',len(linearids), 'unique roads in the list to be processed **\n')
            #print('\n',len(linearids), 'unique roads added to list')
            #Write list to file?

            #Do tasks here.
            #Do primary roads as task 1
            #Do secondary roads as task 2


            #Start timer
            start_time = datetime.now()

            # Create queues
            #ctx = get_context('spawn')
            #cdx = get_context('spawn')

            #task_queue = ctx.Queue(maxsize = self.batch_size)
            #done_queue = cdx.Queue(maxsize = self.batch_size)

            task_queue = Queue(maxsize = self.batch_size)
            done_queue = Queue(maxsize = self.batch_size)
            processes = []

            #Put linearid in queue in batches.
            for j in range(0, len(linearids), self.batch_size):

                pris_batch = linearids[j:j+self.batch_size]


                #pris = [(self.setprocessroadargs, (i)) for i in pris_batch]
                #print('main--', pris)

                #How to put road ids in queue in batches?
                for pri in pris_batch:
                    #check queue size b4 putting in?
                    #if not task_queue.full:
                        #wait
                        #time.sleep(5)
                
                    task_queue.put(pri)
                    #print('pri--- ', pri)

                for i in range(self.processcount):
                    #self.callprocessroad(task_queue, done_queue)
                    #Process(target=self.callprocessroad, args=(task_queue, done_queue, lock)).start()           
                    p = Process(target=self.callprocessroad, args=(task_queue, done_queue, lock))
                    processes.append(p)
                    p.start()

                # completing process
                for p in processes:
                    p.join()

                for i in range(len(pris_batch)):
                    done_queue.get()
                    #Implement wait here to prevent duplicate roads being processed at the same time?
                    #time.sleep(.030)

                # Tell child processes to stop
                #for i in range(self.processcount):
                #    task_queue.put('STOP')

                #print('Operation Completed')

            print('************************')
            print('*                      *')
            print('* Operation Completed! *')
            print('*                      *')
            print('************************')

            end_time = datetime.now()

            print('Duration: {}'.format(end_time - start_time))

        except:
            print('Error: ', sys.exc_info())

                

        

