import subprocess
import os
from ast import literal_eval
from sys import platform

f = open("config.conf", "r")
config = f.read()
f.close()
config = literal_eval(config)
print "will check platform"
if platform == "darwin":
    # OSX
    args = ["osascript", "launch_helper.scpt"]
elif platform == "linux2" or platform == "linux2":
    # Linux
    args = ["./launch_helper.sh", "filler"]
    print "linux detected"
else:
    print "Operating system not yet supported. Please run on OSX or Linux."
    exit(1)
print "platform detected"

args.append("server.py")
args.append(config['network'])
args.append("port")
for server in config['servers']:
    args[-1] = server
    subprocess.call(args)

args = args[:2] + ["network.py"] + [config['network']] + config['servers'] + [str(config["delay"])]
subprocess.call(args)

print "All processes launched."
