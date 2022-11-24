# raft-impl

Hello!

This implementation has three parts:
1. **Servers**: part of the cluster, and can be created by running `python3 raft.py`
2. **Proxy**: what connects clients to the server (you can just use one!), can be created by running `python3 proxy.py`
3. **Client**: a program created to help you interface with the cluster more easily, run `python3 client.py`. Be sure to create the necessary servers + proxies before attempting to send requests!

*You can alter the speed at which the servers run by altering the `SLOW_MODE` constant in `utils.py`!*
