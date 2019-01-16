#!/usr/bin/env bash
# must source the virtual environment first!
echo ...instantiating input ring buffer...
dada_db -b 26214400 -k 0x1234 -n 400 -p

# Obtain list of all *.sub files in dir. For each file launch process, then rename file to *.done
python mwax_mover.py -w /tmp/greg/ -x ".sub" -e "echo 'processing __FILE__'" -r ".done" -m PROCESS_DIR
#dada_diskdb -k 0x1234 -f /tmp/greg/1216447872_02_256_201.sub -s

echo ...destroying input ring buffer
dada_db -k 0x1234 -d
