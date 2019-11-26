import mwax_archive_processor
import mwax_subfile_processor
import argparse
import logging
import logging.handlers
import signal
import sys
import time

MODE_MWAX_CORRELATOR = "MWAX_CORRELATOR"
MODE_MWAX_BEAMFORMER = "MWAX_BEAMFORMER"


class MWAXSubfileDistributor:
    def __init__(self):
        # init the logging subsystem
        self.logger = logging.getLogger('mwax_subfile_distributor')

        # init vars
        self.mode = None
        self.running = False
        self.processors = []
        self.ringbuffer_key = None
        self.numa_node = None

    def initialise(self):

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = "mwax_subfile_distributor: a command line tool which is part of the mwax " \
                             "suite for the MWA. It will perform different tasks based on --mode option.\n" \
                             "In addition, it will automatically archive files in /voltdata and /visdata to the " \
                             "mwacache servers at the Curtin Data Centre. Files in /voltdata/beams will be sent to" \
                             "Fredda.\n"
        parser.add_argument("-m", "--mode", required=True, default=None,
                            choices=[MODE_MWAX_CORRELATOR, MODE_MWAX_BEAMFORMER, ],
                            help=f"Mode to run:\n"
                            f"{MODE_MWAX_CORRELATOR}: watch for new sub files. If correlator then pass onto ringbuffer"
                                 f", if voltage capture then store to disk and disable archiver.\n" 
                            f"{MODE_MWAX_BEAMFORMER}: watch for new sub files, pass onto ringbuffer.\n")

        parser.add_argument("-k", "--key", required=True, default=None,
                            help=f"PSRDADA Ringbuffer key:\n"
                                 f"When mode = {MODE_MWAX_CORRELATOR}, this is the correlator input ringbuffer key.\n"
                                 f"When mode = {MODE_MWAX_BEAMFORMER}, this is the beamformer input ringbuffer key.\n")

        parser.add_argument("-n", "--numa_node", required=True, default=None,
                            help=f"NUMA node to use when copying subfiles to destinations.\n")

        args = vars(parser.parse_args())

        # Check args
        self.mode = args["mode"]
        self.ringbuffer_key = args["key"]
        self.numa_node = args["numa_node"]

        # start logging
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
        self.logger.addHandler(ch)

        self.logger.info("Starting mwax_subfile_distributor processor...")

        if self.mode == MODE_MWAX_CORRELATOR or self.mode == MODE_MWAX_BEAMFORMER:
            processor = mwax_subfile_processor.SubfileProcessor(self.logger, self.mode, self.ringbuffer_key, self.numa_node)

            # Add this processor to list of processors we manage
            self.processors.append(processor)

            processor = mwax_archive_processor.ArchiveProcessor(self.logger, self.mode, "mwacache10", 7700)

            # Add this processor to list of processors we manage
            self.processors.append(processor)

        else:
            self.logger.error("Unknown running mode. Quitting.")
            exit(1)

    def signal_handler(self, signum, frame):
        self.logger.warning(f"Interrupted. Shutting down {len(self.processors)} processors...")
        self.running = False

        # Stop any Processors
        for processor in self.processors:
            processor.stop()

    def start(self):
        self.running = True

        # Make sure we can Ctrl-C / kill out of this
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.info(f"Running in mode {self.mode}")

        for processor in self.processors:
            processor.start()

        while self.running:
            for processor in self.processors:
                for t in processor.worker_threads:
                    if t:
                        if t.isAlive():
                            time.sleep(1)
                        else:
                            self.running = False
                            break

        # Finished
        self.logger.info("Completed Successfully")


if __name__ == '__main__':
    p = MWAXSubfileDistributor()

    try:
        p.initialise()
        p.start()
        sys.exit(0)
    except Exception as e:
        if p.logger:
            p.logger.exception(str(e))
        else:
            print(str(e))