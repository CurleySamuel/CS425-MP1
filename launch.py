import subprocess
import os
from ast import literal_eval
from sys import platform

f = open("config.conf", "r")
config = f.read()
f.close()
config = literal_eval(config)

if platform == "darwin":
    # OSX
    args = ["osascript", "launch_helper.scpt"]
else:
    # Linux tbd
    print "Operating system not yet supported"
    exit(1)
args.append("server.py")
args.append(config['network'])
args.append("port")
for server in config['servers']:
    args[-1] = server
    subprocess.call(args)

args = args[:2] + ["network.py"] + config['servers']
subprocess.call(args)

print "All processes launched."
