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
elif platform == "linux" or platform == "linux2":
    # Linux
    print "Need to write the bash file for Linux."
    exit(1)
else:
    print "Operating system not yet supported. Please run on OSX or Linux."
    exit(1)


args.append("server.py")
args.append(config['network'])
args.append("port")
for server in config['servers']:
    args[-1] = server
    subprocess.call(args)

args = args[:2] + ["network.py"] + [config['network']] + config['servers'] + [str(config["delay"])]
subprocess.call(args)

print "All processes launched."
