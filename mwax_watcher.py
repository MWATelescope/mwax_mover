import inotify.constants
import inotify.adapters
import glob
import mwax_mover
import os


def get_file_list(watch_dir, watch_ext):
    pattern = os.path.join(watch_dir, "*" + watch_ext)
    files = glob.glob(pattern)
    return sorted(files)


def scan_directory(logger, watch_dir, watch_ext, q):
    # Just loop through all files and add them to the queue
    logger.info(f"Scanning {watch_dir} for files matching {'*' + watch_ext}...")

    files = get_file_list(watch_dir, watch_ext)

    logger.info(f"Found {len(files)} files")

    for file in files:
        q.put(file)
        logger.info(f'Added {file} to queue')


class Watcher(object):
    def __init__(self, path='.', q=None, pattern=None, log=None, mode=None):
        self.logger = log
        self.i = inotify.adapters.Inotify()
        self.mode = mode
        self.path = path
        self.watching = False
        self.q = q
        self.pattern = pattern

        if self.mode == mwax_mover.MODE_WATCH_DIR_FOR_NEW:
            self.mask = inotify.constants.IN_CLOSE_WRITE
        elif self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME:
            self.mask = inotify.constants.IN_MOVED_TO

        # Check that the path to watch exists
        if not os.path.exists(self.path):
            raise FileNotFoundError(self.path)

    def start(self):
        self.logger.info(f"Watcher starting on {self.path}/{self.pattern}...")
        self.i.add_watch(self.path, mask=self.mask)
        self.watching = True
        self.do_watch_loop()

    def stop(self):
        self.logger.info(f"Watcher stopping on {self.path}/{self.pattern}...")
        self.i.remove_watch(self.path)
        self.watching = False

    def do_watch_loop(self):
        ext_length = len(self.pattern)
        first_run = True

        while self.watching:
            for event in self.i.event_gen(timeout_s=1, yield_nones=False):
                (_, evt_type_names, evt_path, evt_filename) = event

                filename_len = len(evt_filename)

                # check filename
                if evt_filename[(filename_len-ext_length):] == self.pattern:
                    dest_filename = os.path.join(evt_path, evt_filename)
                    self.q.put(dest_filename)
                    self.logger.info(f'Added {dest_filename} to queue')

            # If we're in NEW or RENAME mode, then scan the folder once we have enqueued any waiting items
            if self.mode == mwax_mover.MODE_WATCH_DIR_FOR_NEW or self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME:
                if first_run:
                    scan_directory(self.logger, self.path, self.pattern, self.q)
                    first_run = False

    def get_status(self):
        return {"watching": self.watching,
                "mode": self.mode,
                "watch_path": self.path,
                "watch_pattern": self.pattern}
