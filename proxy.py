import time

from util import *

import concurrent.futures

from flask import Flask
from flask import jsonify
from flask import request

from threading import Lock

import requests

app = Flask(__name__)

server_id = None
server_ids_in_cluster = []

hot_n_ready = {}
hot_lock = Lock()
request_num = 0

create_request_ack = lambda success, msg : jsonify({"success": success, "msg": msg})

def initialize_proxy_server():
    global server_ids_in_cluster, server_id

    while True:
        try:
            print("enter an id found in proxy_ids.txt")
            server_id = int(input())
            break
        except KeyboardInterrupt:
            print("ok")
            return False
        except:
            print("invalid port number")

    server_ids_in_cluster = read_server_ids()
    return True

def gen_new_request_id():
    global request_num

    request_id = "{}:{}".format(server_id, request_num)
    request_num += 1
    return request_id

@app.route('/send-request', methods=['POST'])
def send_request_from_proxy():
    request_json = request.json

    proxy_request = create_proxy_request(request_json)

    try:
        _ = send_to_cluster(proxy_request)
    except Exception as err:
        return create_request_ack(False, "(proxy:send_request_from_proxy) send to cluster failed, {}".format(err))

    # check the hot n ready for a while (proxy request) for a response
    expiration_time = time.time() + PROXY_TIMEOUT_S
    time_since_last_check = time.time()
    while time.time() < expiration_time:
        if time_since_last_check - PROXY_QUEUE_INTERVAL_S < time.time():
            time_since_last_check = time.time()

            if not response_received(proxy_request):
                continue

            # safe pop the item off the hot n ready
            pop_response(proxy_request)

            # return proxy response args as a json
            # always true because things only get added to hot n ready if true, maybe change this?
            return REQUEST_SUCCESS
    
    return REQUEST_TIMEOUT

def response_received(proxy_request):
    if not isinstance(proxy_request, ClientRequestArgs):
        raise Exception("(proxy:response_received) invalid request seeking response")
    
    return proxy_request.request_id in hot_n_ready

def pop_response(proxy_request):
    if not isinstance(proxy_request, ClientRequestArgs):
        raise Exception("(proxy:get_response) invalid request seeking response")
    if not response_received(proxy_request):
        raise Exception("(proxy:get_response) request has no response")
    
    return safe_consume(proxy_request.request_id)

@app.route('/queue', methods=['POST'])
def reply_from_server():
    request_json = request.json
    
    # check if the request is well-formed
    try:
        data = request_json["data"]

        args = dict_to_dataclass(data, ClientRequestResponseArgs)
    except Exception as err:
        raise Exception("(proxy:reply_from_server) {}".format(err))
    
    # see if its a success, if so add to hot n ready
    if args.success:
        safe_serve(args.request_id, args.msg)

    # return request success json
    return REQUEST_SUCCESS

def safe_serve(k, v):
    with hot_lock:
        hot_n_ready[k] = v

def safe_consume(x):
    with hot_lock:
        hot_n_ready.pop(x)


def send_to_cluster(args):
    with concurrent.futures.ThreadPoolExecutor(max_workers= MAX_THREADS_SPAWNED) as executor:
        executor.map(lambda x : send_to_server(x, args), server_ids_in_cluster)

def send_to_server(dest_id, args):
    url = server_id_to_address(dest_id)
    json_data = {"origin_id": server_id, "request_type": PROXY, "cur_term": None, "data": dataclass_to_dict(args)}

    try:
        requests.post("{}/queue".format(url), json= json_data, timeout= REQUEST_TIMEOUT_S)
    except requests.exceptions.Timeout:
        return REQUEST_TIMEOUT
    except Exception as err:
        return create_request_ack(False, err)
    
    return REQUEST_SUCCESS

def create_proxy_request(data):
    try:
        payload = data["payload"]
        request_id = gen_new_request_id()

        return ClientRequestArgs(request_id, payload)
    except Exception as err:
        raise Exception("(proxy:parse_json) error parsing json, {}".format(err))

if __name__ == "__main__":
    setup_success = initialize_proxy_server()

    if setup_success:
        app.run(debug= False, port= server_id)
