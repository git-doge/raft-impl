import requests

from util import server_id_to_address

def main():
    setup_success = False

    print("welcome! be sure to start up at least a quorum of servers with IDs from server_ids.txt first, as well as at least one proxy.")
    while True:
        try:
            print("enter a live proxy's id:")
            proxy = int(input())
            setup_success = True
            break
        except KeyboardInterrupt:
            print("ok")
            break
        except:
            continue
    
    if setup_success:
        try:
            print("type some requests to send to the cluster!")
            while True:
                payload = input().strip()
                print("")
                print("tossing '{}' into the void...".format(payload))
                print("feel free to send more requests, they'll be sent after the ones before it have been processed!")
                _ = requests.post("{}/send-request".format(server_id_to_address(proxy)), json= {"payload": payload})
        except KeyboardInterrupt:
            print("bye!")

if __name__ == "__main__":
    main()