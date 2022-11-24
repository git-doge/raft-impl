import dataclasses
from dataclasses import dataclass
from dataclasses_json import dataclass_json

import typing
from typing import Optional

import random as ra
import json

#################################################################
# USE SLOW_FACTOR TO ALTER THE SPEED AT WHICH EVERYTHING IS RUN #
# 0.5 = i want results NOW                                      #
# 1 = fast                                                      #
# 3 = reasonably fast                                           #
# 10 = debug mode                                               #
#################################################################

SLOW_FACTOR = 3

INIT = "initialize"
SEND_HEARTBEAT = "heartbeat"
CHECK_ELECTION_TIMER = "election timer"
VOTE_REQUEST = "vote request"
VOTE_REQUEST_RESPONSE = "vote request response"
APPEND_ENTRIES = "append entries"
APPEND_ENTRIES_RESPONSE = "append entries response"
UPDATE = "update"
PROXY = "proxy"
PROXY_RESPONSE = "proxy response"

START_LOG = "start log"

LOG_INCONSISTENCY_ERROR = "log inconsistency"
TIMEOUT_ERROR = "timeout"

QUEUE_INTERVAL_S = 0.1 * SLOW_FACTOR
REQUEST_TIMEOUT_S = 1 * SLOW_FACTOR
PROXY_TIMEOUT_S = 5 * SLOW_FACTOR
PROXY_QUEUE_INTERVAL_S = 0.1 * SLOW_FACTOR

MAX_THREADS_SPAWNED = 4
REQUEST_SUCCESS = json.dumps({'success' : True})
REQUEST_TIMEOUT = json.dumps({'success' : True, 'msg': TIMEOUT_ERROR})

SERVER_LIST_FILENAME = "server_ids.txt"

def read_server_ids():
    server_ids_in_cluster = []
    try:
        with open(SERVER_LIST_FILENAME, "r") as f:
            for line in f:
                s_id = int(line.strip())
                server_ids_in_cluster.append(s_id)
            
            f.close()
    except Exception as err:
        raise Exception("(get_all_servers) error reading server list, {}".format(err))
    
    return server_ids_in_cluster

def server_id_to_address(s_id):
    return "http://127.0.0.1:{}".format(s_id)

@dataclass_json
@dataclass
class QueueItem:
    origin_id: int
    request_type: str
    cur_term: int
    data: 'typing.Any'

@dataclass_json
@dataclass
class LogEntry:
    data: 'typing.Any'
    term: int

@dataclass_json
@dataclass
class InitArgs:
    server_id: int

@dataclass_json
@dataclass
class AppendEntriesArgs:
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit_index: int

@dataclass_json
@dataclass
class AppendEntriesResponseArgs:
    success: bool
    match_index: int
    msg: Optional[str] = None

@dataclass_json
@dataclass
class RequestVoteArgs:
    last_log_index: int
    last_log_term: int

@dataclass_json
@dataclass
class RequestVoteResponseArgs:
    vote_granted: bool
    msg: Optional[str] = None

@dataclass_json
@dataclass
class ClientRequestArgs:
    request_id: 'typing.Any'
    payload: 'typing.Any'

@dataclass_json
@dataclass
class ClientRequestResponseArgs:
    request_id: 'typing.Any'
    success: bool
    msg: Optional[str] = None

def dataclass_to_dict(x):
    try:
        return None if x is None else dataclasses.asdict(x)
    except Exception as err:
        raise Exception("(dataclass_to_json) {}".format(err))

def dict_to_dataclass(x, class_type):
    if class_type is None:
        return None
    
    try:
        return class_type.from_json(json.dumps(x))
    except Exception as err:
        return "(json_to_dataclass) conversion from {} to {} failed with error {}".format(x, class_type, err)