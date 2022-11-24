import queue
import requests
import time

from flask import Flask
from flask import request

from util import *

from threading import Thread, Lock
import concurrent.futures

app = Flask(__name__)
ELECTION_TIMER_TIMEOUT_S = SLOW_FACTOR * ra.uniform(350 * 10, 450 * 10) / 1000

queue_last_checked = 0
q_lock = Lock()
q = []

election_timer_last_set = 0

server_id = None
cur_state = None
cur_term = 0
voted_for = None
log = [LogEntry(START_LOG, -1)]

commit_index = 0
last_applied = 0

# INIT
def initialize_server(args):
    global server_id, q, election_timer_last_set, cur_state, MAJORITY_SIZE

    data = args.data

    if not isinstance(data, InitArgs):
        raise Exception("(initialize_server) given arguments {} of invalid type".format(data))
    
    cur_state = None

    server_id = data.server_id
    get_all_servers()
    MAJORITY_SIZE = len(server_ids_in_cluster) // 2 + 1

    election_timer_last_set = time.time()
    queue_safe_append(QueueItem(server_id, CHECK_ELECTION_TIMER, None, None))

    initialize_state_machine()
    queue_safe_append(QueueItem(server_id, UPDATE, cur_term, None))

server_ids_in_cluster = None
MAJORITY_SIZE = -1

def get_all_servers(include_self= False):
    global server_ids_in_cluster

    if server_ids_in_cluster is None:
        server_ids_in_cluster = read_server_ids()
    
    if include_self:
        return server_ids_in_cluster
    else:
        return [x for x in server_ids_in_cluster if x != server_id]

def get_address_by_server_id(s_id):
    return server_id_to_address(s_id)

def initialize_state_machine():
    open("{}.txt".format(server_id), "w").close()

# HEARTBEATS
def send_heartbeat(args):
    if isinstance(cur_state, Leader):
        cur_state.broadcast_heartbeats()
        queue_safe_append(QueueItem(server_id, SEND_HEARTBEAT, cur_term, None))

# ELECTIONS
def check_election_timer(args):
    global cur_state

    if not isinstance(cur_state, Leader):
        if election_timer_expired():
            cur_state = Candidate()
    
    queue_safe_append(QueueItem(server_id, CHECK_ELECTION_TIMER, None, None))

def election_timer_expired():
    return time.time() - election_timer_last_set >= ELECTION_TIMER_TIMEOUT_S

def reset_election_timer():
    global election_timer_last_set
    election_timer_last_set = time.time()

def process_vote_request(args):
    # this is possible for all states if a candidate runs an election on the same term as the machine
    cur_state.process_vote_request(args)

def respond_to_vote_request(s_id, vote_granted, msg= None):
    send_request(s_id, VOTE_REQUEST_RESPONSE, RequestVoteResponseArgs(vote_granted, msg))

def process_vote_request_response(args):
    # not accounting for leader asserting itself, since it will do so via heartbeats anyway
    # if follower or leader, request is outdated/irrelevant
    if isinstance(cur_state, Candidate):
        cur_state.process_vote_request_response(args)

# APPEND ENTRIES
def process_append_entries(args):
    if isinstance(cur_state, Leader):
        raise Exception("(process_append_entries) append entries processed as leader")
    
    cur_state.process_append_entries(args)

def respond_to_append_entries(s_id, accepted, msg= None):
    send_request(s_id, APPEND_ENTRIES_RESPONSE, AppendEntriesResponseArgs(accepted, len(log) - 1, msg))

def process_append_entries_response(args):
    if not isinstance(cur_state, Leader):
        return
    cur_state.process_append_entries_response(args)

def process_update(args):
    update_state_machine()
    queue_safe_append(QueueItem(server_id, UPDATE, None, None))

def update_state_machine():
    global last_applied
    while last_applied < commit_index:
        last_applied += 1
        commit_entry(log[last_applied])

def commit_entry(entry):
    try:
        with open("{}.txt".format(server_id), "a") as f:
            f.write("{}\n".format(entry.data))
    except Exception as err:
        raise Exception("(commit_entry) error {} committing entry {}".format(err, entry))

# PROXY
def process_proxy_request(args):
    origin_id, data = args.origin_id, args.data
    if not isinstance(data, ClientRequestArgs):
        raise Exception("(process_proxy_request) invalid args passed in")

    if not isinstance(cur_state, Leader):
        # reject, maybe implement forwarding later
        try:
            return respond_to_proxy(origin_id, data.request_id, False, "server {} is not leader".format(server_id))
        except Exception as err:
            raise Exception("(process_proxy_request) rejection failed, {}".format(err))
    
    try:
        cur_state.process_proxy_request(args)
    except Exception as err:
        raise Exception("(process_proxy_request) {}".format(err))

def respond_to_proxy(s_id, request_id, success, msg= None):
    try:
        send_request(s_id, PROXY_RESPONSE, ClientRequestResponseArgs(request_id, success, msg))
    except Exception as err:
        raise Exception("(respond_to_proxy) {}".format(err))

# STATE CLASSES
class Follower:
    def __init__(self) -> None:
        reset_election_timer()
    
    def process_append_entries(self, args):
        global commit_index

        origin_id, data = args.origin_id, args.data
        origin_prev_log_index, origin_prev_log_term, entries, leader_commit_index = data.prev_log_index, data.prev_log_term, data.entries, data.leader_commit_index

        print("DEBUG", entries)

        reset_election_timer()
        
        prev_log_entry = LogEntry(None, None) if origin_prev_log_index >= len(log) else log[origin_prev_log_index]
        prev_log_term = prev_log_entry.term
        if prev_log_term != origin_prev_log_term:
            respond_to_append_entries(origin_id, False, msg= LOG_INCONSISTENCY_ERROR)
            return
        
        # move forward through all matching entries, pruning entries
        # this is to prevent earlier requests that lagged from mass deleting log entries added from other requests
        i = origin_prev_log_index + 1
        while 0 < len(entries) and i < len(log) and log[i] == entries[0]:
            entries.pop(0)
            i += 1
        
        # this check is necessary if you attempt to add entries to the middle of a log (due to lagging requests)
        if len(entries) > 0:
            # delete all conflicts after prev log index
            while len(log) > i:
                log.pop()
            
            # extend with remaining entries
            log.extend(entries)

        # commit entries as needed
        if commit_index < leader_commit_index:
            commit_index = min(leader_commit_index, len(log) - 1)
        
        respond_to_append_entries(origin_id, True)
    
    def process_vote_request(self, args):
        global voted_for
        origin_id, origin_cur_term, data = args.origin_id, args.cur_term, args.data
        origin_last_log_index, origin_last_log_term = data.last_log_index, data.last_log_term

        if origin_cur_term < cur_term:
            # this should be caught earlier
            raise Exception("(Follower:process_vote_request) invalid vote request processed")
        
        print("DEBUG", "asked to vote for {}, voted for {}".format(origin_id, voted_for))

        if (self.should_vote_for(origin_id, origin_last_log_index, origin_last_log_term)):
            # grant vote
            print("DEBUG", "VOTING FOR {}".format(origin_id))

            voted_for = origin_id
            respond_to_vote_request(origin_id, True)
        elif not self.should_vote_for(origin_id, origin_last_log_index, origin_last_log_term):
            # reject
            respond_to_vote_request(origin_id, False)
        else:
            raise Exception("(Follower:process_vote_request) incomplete decision tree")
    
    def should_vote_for(self, origin_id, origin_last_log_index, origin_last_log_term):
        last_log_entry = log[-1]
        last_log_index, last_log_term = len(log) - 1, last_log_entry.term

        return (
            (voted_for is None or voted_for == origin_id) and
            not (
                (last_log_term > origin_last_log_term) or
                (last_log_term == origin_last_log_term and last_log_index > origin_last_log_index)
            )
        )

class Candidate:
    def __init__(self) -> None:
        global cur_term, election_timer_last_set, voted_for

        cur_term += 1
        voted_for = server_id
        self.votes = set([server_id])
        reset_election_timer()

        self.send_vote_requests()
        
        # TODO: double check if there's more
    
    def process_append_entries(self, args):
        global cur_state
        cur_state = Follower()

        try:
            # forwards request to follower implementation
            cur_state.process_append_entries(args)
        except Exception as err:
            raise Exception("(Candidate:process_append_entries) {}".format(err))

    def send_vote_requests(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers= MAX_THREADS_SPAWNED) as executor:
            executor.map(self.send_vote_request, get_all_servers())

    def send_vote_request(self, dest_id):
        last_log_entry = log[-1]
        send_request(dest_id, VOTE_REQUEST, RequestVoteArgs(len(log) - 1, last_log_entry.term))
    
    def process_vote_request(self, args):
        origin_id, origin_cur_term = args.origin_id, args.cur_term
        if cur_term != origin_cur_term:
            # a candidate should only consider a vote request if it's from the same term
            raise Exception("(Candidate:process_vote_request) invalid vote request processed")
        
        # lmao
        respond_to_vote_request(origin_id, False)
    
    def process_vote_request_response(self, args):
        origin_id, origin_cur_term, data = args.origin_id, args.cur_term, args.data

        if cur_term != origin_cur_term:
            raise Exception("(Candidate:process_vote_request_response) processed invalid vote request response")
        
        # did they even vote for you lmao
        if not data.vote_granted:
            return

        self.votes.add(origin_id)
        print("DEBUG", "TERM {} ELECTION, {} VOTES".format(cur_term, self.votes))
        
        if self.won_election():
            self.join_illuminati()
    
    def won_election(self):
        return len(self.votes) >= MAJORITY_SIZE
    
    def join_illuminati(self):
        global cur_state
        cur_state = Leader()

class Leader:
    def __init__(self) -> None:
        server_list = get_all_servers()

        self.next_index = {x : len(log) for x in server_list}
        self.match_index = {x : 0 for x in server_list}

        queue_safe_append(QueueItem(server_id, SEND_HEARTBEAT, cur_term, None))
    
    def broadcast_heartbeats(self):
        self.broadcast_append_entries()
    
    def broadcast_append_entries(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers= MAX_THREADS_SPAWNED) as executor:
            executor.map(self.send_append_entries, get_all_servers())

    def send_append_entries(self, dest_id):
        if dest_id == server_id:
            raise Exception("(Leader:send_append_entries) attempting to send request to yourself")
        
        prev_log_item = log[self.next_index[dest_id] - 1]
        prev_log_term = prev_log_item.term

        print("DEBUG", dest_id, self.next_index[dest_id], log[self.next_index[dest_id]:])
        send_request(dest_id, APPEND_ENTRIES, AppendEntriesArgs(self.next_index[dest_id] - 1, prev_log_term, log[self.next_index[dest_id]:], commit_index))
    
    def process_append_entries_response(self, args):
        origin_id, data = args.origin_id, args.data
        success, follower_match_index, msg = data.success, data.match_index, data.msg

        print("DEBUG", origin_id, success, follower_match_index, msg)

        if not success:
            if msg == LOG_INCONSISTENCY_ERROR:
                self.next_index[origin_id] = max(follower_match_index + 1, self.next_index[origin_id] - 1)

                if self.next_index[origin_id] < 0:
                    raise Exception("(Leader:process_append_entries_response) invalid next index")
                
                self.send_append_entries(origin_id)
            else:
                raise Exception("(Leader:process_append_entries_response) invalid failure message")
        else:
            self.next_index[origin_id] = follower_match_index + 1
            self.match_index[origin_id] = follower_match_index

            self.update_commit_index()
    
    def update_commit_index(self):
        global commit_index

        majority_match_indices = sorted(list(self.match_index.values()) + [len(log) - 1])[:-1 * MAJORITY_SIZE + 1]
        
        for i in majority_match_indices:
            if i > commit_index and log[i].term == cur_term:
                commit_index = i
    
    def process_vote_request(self, args):
        origin_id, origin_cur_term = args.origin_id, args.cur_term
        if cur_term != origin_cur_term:
            # a leader should only consider a vote request if it's from the same term
            raise Exception("(Leader:process_vote_request) invalid vote request processed")
        
        # sike lol
        respond_to_vote_request(origin_id, False)
    
    def process_proxy_request(self, args):
        origin_id, data = args.origin_id, args.data
        request_id, payload = data.request_id, data.payload
        
        self.add_log_entry(LogEntry(payload, cur_term))
    
    def add_log_entry(self, x):
        if not isinstance(x, LogEntry):
            raise Exception("(Leader:add_log_entry) invalid log entry type")
        
        # i don't think i need a lock for this, since everything is serial due to the queue?
        log.append(x)
        self.broadcast_append_entries()


# QUEUE
def queue_safe_append(x):
    with q_lock:
        q.append(x)

def pull_from_queue():
    while True:
        if queue_timer_expired():
            if len(q) > 0:
                debug()
                with q_lock:
                    item = q.pop(0)
                
                try:
                    address_queue_item(item)
                except Exception as err:
                    print("(pull_from_queue) {}, ignoring request".format(err))
            
            reset_queue_timer()

def queue_timer_expired():
    return time.time() - queue_last_checked >= QUEUE_INTERVAL_S

def reset_queue_timer():
    global queue_last_checked
    queue_last_checked = time.time()

def address_queue_item(item):
    global cur_state, cur_term, voted_for
    origin_id, request_type, origin_cur_term, args = item.origin_id, item.request_type, item.cur_term, item.data

    if not verify_args_matches_string(request_type, args):
        raise Exception("(address_queue_item) invalid arguments {} request type {}".format(args, request_type))
    
    try:
        # term is None if request is from proxy or persistent
        if origin_cur_term is not None:
            if origin_cur_term < cur_term:
                reject_request(origin_id, request_type)
                return
            if origin_cur_term > cur_term:
                cur_term = origin_cur_term
                voted_for = origin_id
                cur_state = Follower()
        string_to_queue_func(request_type)(item)
    except Exception as err:
        raise Exception("(address_queue_item) {}".format(err))

def debug():
    print("")

    print("state:", type(cur_state).__name__)
    print("term:", cur_term)

    cur_q = [(x.request_type, x.origin_id) for x in q]
    print("q:", cur_q)

    print("log:", log)

@app.route('/queue', methods=['POST'])
def add_to_queue():
    request_json = request.json
    q_item = json_to_queue_item(request_json)

    queue_safe_append(q_item)
    
    return REQUEST_SUCCESS

# UTILS
def send_request(dest_id, request_type, args):
    url = get_address_by_server_id(dest_id)
    json_data = {"origin_id": server_id, "request_type": request_type, "cur_term": cur_term, "data": dataclass_to_dict(args)}
    
    request = requests.post("{}/{}".format(url, get_url_extension(queue)), json= json_data, timeout= REQUEST_TIMEOUT_S)
    
    return request

def get_url_extension(request_type):
    return "queue"

def reject_request(s_id, request_type, msg= None):
    if request_type == APPEND_ENTRIES:
        respond_to_append_entries(s_id, False, msg)
    elif request_type == VOTE_REQUEST:
        respond_to_vote_request(s_id, False, msg)
    elif request_type == PROXY:
        raise Exception("(reject_request) proxy rejections not supported")
    else:
        pass

def forward_queue_item(dest_id, q_item):
    url = get_address_by_server_id(dest_id)

    request = requests.post(url, json= dataclass_to_dict(q_item), timeout= REQUEST_TIMEOUT_S)

    return request

def json_to_queue_item(x):
    try:
        origin_id = x["origin_id"]
        request_type = x["request_type"]
        data = x["data"]
        cur_term = x["cur_term"]

        return QueueItem(origin_id, request_type, cur_term, dict_to_dataclass(data, string_to_dataclass(request_type)))
    except Exception as err:
        raise Exception("(unpack_json) given {}, {}".format(x, err))

def verify_args_matches_string(s, x):
    args_class = string_to_dataclass(s)
    return (args_class is None and x is None) or (isinstance(x, args_class))

def string_to_dataclass(s):
    if s == INIT:
        return InitArgs
    elif s == SEND_HEARTBEAT:
        return None
    elif s == CHECK_ELECTION_TIMER:
        return None
    elif s == VOTE_REQUEST:
        return RequestVoteArgs
    elif s == VOTE_REQUEST_RESPONSE:
        return RequestVoteResponseArgs
    elif s == APPEND_ENTRIES:
        return AppendEntriesArgs
    elif s == APPEND_ENTRIES_RESPONSE:
        return AppendEntriesResponseArgs
    elif s == UPDATE:
        return None
    elif s == PROXY:
        return ClientRequestArgs
    else:
        raise Exception("(class_from_string) invalid string type \"{}\"".format(s))

def dataclass_to_string(x):
    if isinstance(x, InitArgs):
        return INIT
    elif isinstance(x, RequestVoteArgs):
        return VOTE_REQUEST
    elif isinstance(x, RequestVoteResponseArgs):
        return VOTE_REQUEST_RESPONSE
    elif isinstance(x, AppendEntriesArgs):
        return APPEND_ENTRIES
    elif isinstance(x, AppendEntriesResponseArgs):
        return APPEND_ENTRIES_RESPONSE
    elif isinstance(x, ClientRequestArgs):
        return PROXY
    else:
        raise Exception("(dataclass_to_string) invalid class type, or None: \"{}\"".format(x))

def string_to_queue_func(s):
    if s == INIT:
        return initialize_server
    elif s == SEND_HEARTBEAT:
        return send_heartbeat
    elif s == CHECK_ELECTION_TIMER:
        return check_election_timer
    elif s == VOTE_REQUEST:
        return process_vote_request
    elif s == VOTE_REQUEST_RESPONSE:
        return process_vote_request_response
    elif s == APPEND_ENTRIES:
        return process_append_entries
    elif s == APPEND_ENTRIES_RESPONSE:
        return process_append_entries_response
    elif s == UPDATE:
        return process_update
    elif s == PROXY:
        return process_proxy_request
    else:
        raise Exception("(string_to_queue_func) invalid function type \"{}\"".format(s))

if __name__ == "__main__":
    port_num = None
    setup_success = False

    while True:
        print("enter a port number found in server_ids.txt!")
        try:
            print("port number:")
            port_num = int(input())
            setup_success = True
            break
        except KeyboardInterrupt:
            print("bye")
            break
        except:
            print("invalid port number")
    
    if setup_success:
        queue_safe_append(QueueItem(server_id, INIT, cur_term, InitArgs(port_num)))

        pq = Thread(target= pull_from_queue, daemon= True)
        pq.start()

        app.run(debug= False, port= port_num)
