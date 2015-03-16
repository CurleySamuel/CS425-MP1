import subprocess
import os
from ast import literal_eval
from sys import platform
import pickle

f = open("config.conf", "r")
config = f.read()
f.close()
config = literal_eval(config)
if platform == "darwin":
    # OSX
    args = ["osascript", "launch_helper.scpt"]
    print "Running launch script for OSX"
elif platform == "linux" or platform == "linux2":
    # Linux
    args = ["./launch_helper.sh", "filler"]
    print "Running launch script for Linux"
else:
    print "Operating system not yet supported. Please run on OSX or Linux."
    exit(1)

config["delay"] = pickle.dumps(config["delay"])
args.append("server.py")
args.append(config['network'])
args.append("port")
for server in config['servers']:
    args[-1] = server
    subprocess.call(args)

print "All processes launched. Launching network now."
args = ["python"] + ["network.py"] + [config['network']] + config['servers'] + [config["delay"]]
subprocess.call(args)

