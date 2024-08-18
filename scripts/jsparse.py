#!/usr/bin/env python
import sys
import json
import pprint

with open(sys.argv[1]) as f:
    js = json.load(f)
    for x in range(2, len(sys.argv)):
        if sys.argv[x].startswith("string:"):
            js = js[sys.argv[x].split(":")[1]]
        else:
            try:
                js = js[int(sys.argv[x])]
            except:
                js = js[sys.argv[x]]
    pprint.pprint(js)
    
