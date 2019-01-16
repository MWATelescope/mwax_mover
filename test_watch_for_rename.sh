#!/usr/bin/env bash
echo ...instantiating input ring buffer...
dada_db -b 26214400 -k 0x1234 -n 400 -p

python mwax_sub2db.py -w /tmp/greg/ -x ".sub" -e "/usr/local/bin/dada_diskdb -k 0x1234 -f __FILE__ -s" -r ".done" -m WATCH_DIR_FOR_RENAME
#dada_diskdb -k 0x1234 -f /tmp/greg/1216447872_02_256_201.sub -s

echo ...destroying input ring buffer
dada_db -k 0x1234 -d
