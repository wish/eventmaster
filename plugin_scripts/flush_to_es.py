import argparse
from collections import defaultdict
import datetime
import requests
import socket
import sys
import time
import ujson

from elasticsearch import Elasticsearch
from eventmaster_pb2 import *
from eventmaster_pb2_grpc import *

BATCH_LIMIT = 200

parser = argparse.ArgumentParser(description='Configuration settings')
parser.add_argument('--es_service_name',
        help='Service name of elasticsearch client (used for service lookup')
parser.add_argument('--es_ips', nargs='+', help='Ip addresses of elasticsearch')
parser.add_argument('--es_port', help='Port of elasticsearch')
parser.add_argument('--em_addr', help='Addr of Eventmaster server')
parser.add_argument('--start_time', help='Unix timestamp of time to start flushing events')
opts = parser.parse_args()

if opts.em_addr is None:
    sys.stderr.write('No Eventmaster address provided')
    sys.exit(1)

if opts.es_port is None:
    sys.stderr.write('Must specify port of Elasticsearch')
    sys.exit(1)

# use service discovery to find ip addrs of elasticsearch
if opts.es_service_name is not None:
    try:
        resp = requests.get(
            "http://169.254.169.254/latest/meta-data/placement/availability-zone",
            timeout=5)
        dc = resp.content.strip() % "%s."
    except Exception as e:
        print "Error getting dc", e
        print "Using empty string as dc"
        dc = ""
    service_name = "%s.service.%sconsul." % (opts.es_service_name, dc)
    addrs = socket.getaddrinfo(service_name, 80)
    ips = list()
    for addr in addrs:
        sockaddr = addr[4]
        ips.append(sockaddr[0])
elif opts.es_ips is not None:
    ips = opts.es_ips
else:
    sys.stederr.write('Must provide service name for servicelookup or ip addrs of elasticsearch')
    sys.exit(1)

es = Elasticsearch(hosts=[{'host':ip, 'port':opts.es_port} for ip in ips])
print [{'host':ip, 'port':opts.es_port} for ip in ips]

channel = grpc.insecure_channel(opts.em_addr)
stub = EventMasterStub(channel)

def get_index(event_time):
    suffix = datetime.datetime.fromtimestamp(event_time).strftime('%Y-%m-%d')
    return "eventmaster-%s" % suffix

if opts.start_time is not None:
    start_time = int(opts.start_time)
else:
    # start flushing events from 30 mins ago by default
    start_time = int(time.time())-1800

while 1:
    try:
        if int(time.time()) - start_time < 5:
            # sleep for 5 seconds to prevent spamming
            print "All caught up, sleeping for 5 seconds..."
            time.sleep(5)
        print "Flushing events from time:", start_time

        ids = list()
        cur_end_time=int(time.time())
        for id in stub.GetEventIds(TimeQuery(start_event_time=start_time,
            end_event_time=cur_end_time, limit=BATCH_LIMIT, ascending=True)):
            ids.append(id.event_id)
        print "Retrieved", len(ids), "items"

        indexed_events = defaultdict(list)
        for id in ids:
            event = {}
            result = stub.GetEventById(EventId(event_id=id))
            if result is None:
                continue
            if isinstance(result, grpc.RpcError):
                continue
            event['event_id'] = result.event_id
            event['topic_name'] = result.topic_name
            event['dc'] = result.dc
            event['host'] = result.host
            event['event_time'] = result.event_time
            if result.parent_event_id is not None:
                event['parent_event_id'] = result.parent_event_id
            if result.tag_set is not None:
                event['tag_set'] = list()
                for tag in result.tag_set:
                    event['tag_set'].append(tag)
            if result.target_host_set is not None:
                event['target_host_set'] = list()
                for thost in result.target_host_set:
                    event['target_host_set'].append(thost)
            if result.user is not None:
                event['user'] = result.user
            if result.data is not None:
                event['data'] = ujson.loads(result.data)

            indexed_events[get_index(result.event_time)].append(event)

        # no more elements left in current time frame
        if len(ids) < BATCH_LIMIT:
            start_time = cur_end_time

        for index, events in indexed_events.iteritems():
            print "Indexing %d events in index %s" % (len(events), index)
            if not es.indices.exists(index):
                res = es.indices.create(index=index)
                print "Create index response:", res
            bulk_data = []
            for evt in events:
                op_dict = {
                    "index": {
                        "_index": index,
                        "_type": "event",
                        "_id": evt['event_id']
                    }
                }
                bulk_data.append(op_dict)
                bulk_data.append(evt)
                start_time = max(start_time, evt['event_time'])
            res = es.bulk(index=index, body=bulk_data)
            print "Bulk Response:", res
    except Exception as e:
        sys.stderr.write(str(e))
        break
