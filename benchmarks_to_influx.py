from __future__ import print_function

import os
import csv
import time

def to_lines(f, ts):
    build = f.split('-')[0][1:]
    with open(f) as fh:
        cr = csv.reader(fh, delimiter=',', quotechar='"')
        headers = next(cr)
        bench_idx = headers.index('Benchmark')
        unit_idx = headers.index('Unit')
        score_idx = headers.index('Score')
        params = []
        for i, h in enumerate(headers):
            if h.startswith('Param: '):
                param_name = h.split(':')[1].strip()
                params.append((param_name, i))
        for l in cr:
            if not l[unit_idx] == 'us/op':
                continue
            name_parts = l[bench_idx].split('.')[-2:]
            tags = {
                'algo': name_parts[0],
                'test': name_parts[1],
                'build': build
            }
            for (pname, pidx) in params:
                if len(l[pidx]):
                    tags[pname] = l[pidx]
            data = {
                'value': l[score_idx],
                'ts': ts,
                'tags': tags
            }
            yield data

def to_influx(data):
    line = 'benchmark'
    for tag_name,tag_value in data['tags'].iteritems():
        line += ',{}={}'.format(tag_name, tag_value)
    line += ' value={}'.format(data['value'])
    line += ' {}'.format(int(data['ts'] * 1e9))
    return line

def cat_files():
    files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    base_ts = int(time.time()) - len(files)
    for i, f in enumerate(files):
        for line in to_lines(f, base_ts + i):
            yield to_influx(line)

def dump_influx(fh):
    for line in cat_files():
        print(line, file=fh)

if __name__ == '__main__':
    import sys
    if len(sys.argv) >= 2:
        with open(sys.argv[1], 'wb') as fh:
            dump_influx(fh)
    else:
        dump_influx(sys.stdout)
