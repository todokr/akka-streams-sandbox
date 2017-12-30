#!/usr/bin/python
# coding: UTF-8

import random
import itertools
import json
import io

with io.open('sample.json', 'wb') as f:
    for i in itertools.islice(itertools.cycle([1,2,3,4,5]), 10000):
        dict = {"id": i, "score": random.random() * 100}
        str = json.dumps(dict)
        f.write(str + '\n')
