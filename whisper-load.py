#!/usr/bin/env python

import os
import sys
import signal
import optparse

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
try:
   signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except AttributeError:
   #OS=windows
   pass

option_parser = optparse.OptionParser(
    usage='''%prog src_path_file out_path_file ''')

option_parser.add_option('--overwrite', default=False, action='store_true')

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_help()
  sys.exit(1)

src_path_file = args[0]
path = args[1]

def load_meta(f_name):
    info_dict = {"meta":{}}
    with open(f_name, 'r') as fp:
        sec_d = None
        sec = ""
        arc_no = 0
        for line in fp:
            #print(line)
            if line.find("Meta data:") == 0:
                sec_d = info_dict.get("meta")
                sec = "meta"
                continue
            elif line.find(" info:") > 0:
                arc_no = line.split(" ")[1]
                sec_d = {}
                info_dict["arch_" + arc_no] = sec_d
                continue
            elif line.find(" data:") > 0:
                break

            if len(line.strip()) > 0:
                k_v = line.split(":")
                sec_d[k_v[0].strip()] = k_v[1].strip()
    return info_dict
    
meta_dict = load_meta(src_path_file)
ar_dict = [el for el in meta_dict.items() if el[0].find("arch_") > -1]
archives = []
for (ar, params) in ar_dict:
    for (key,value) in params.items():
        try:
            params[key] = int(value)
        except:
            continue
    archive = params
    archive["secondsPerPoint"] = archive["seconds per point"]
    del archive["seconds per point"]
    archives.append((archive["secondsPerPoint"],archive["points"]))
print "archives==",archives
if os.path.exists(path) and options.overwrite:
    print 'Overwriting existing file: %s' % path
    os.unlink(path)

print meta_dict["meta"]
xFilesFactor = float(meta_dict["meta"]["xFilesFactor"])
maxRetention = meta_dict["meta"]["max retention"]
aggregationMethod = meta_dict["meta"]["aggregation method"]
try:
  whisper.create(path, archives, xFilesFactor=xFilesFactor, aggregationMethod=aggregationMethod)
except whisper.WhisperException, exc:
  raise SystemExit('[ERROR] %s' % str(exc))

size = os.stat(path).st_size
print 'Created: %s (%d bytes)' % (path,size)


def load_data(f_name, dest_file):
    with open(f_name, 'r') as fp:
        start = False
        for line in fp:
            datapoints = []
            if start == False:
                if line.find("Archive ") == 0 and line.find(" data:") > 0:
                    start = True
            else:
                datas = line.split(" ")
                if len(datas) == 3 and datas[0] != 'Archive':
                    datapoints.append((datas[1][:-1], datas[2]))
                    #print datapoints
                    whisper.update_many(dest_file, datapoints)


load_data(src_path_file, path)
