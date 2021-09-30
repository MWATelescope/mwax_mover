from mwax_mover import mwax_mover, utils
import inotify.constants
import inotify.adapters
import os


class Watcher(object):
    def __init__(self, path: str, q, pattern: str, log, mode, recursive):
        self.logger = log
        self.i = None
        self.recursive = recursive
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
        if self.recursive:
            self.logger.info(f"Watcher starting on {self.path}/*{self.pattern} and all subdirectories...")
            self.i = inotify.adapters.InotifyTree(self.path, mask=self.mask)
        else:
            self.logger.info(f"Watcher starting on {self.path}/*{self.pattern}...")
            self.i = inotify.adapters.Inotify()
            self.i.add_watch(self.path, mask=self.mask)

        self.watching = True
        self.do_watch_loop()

    def stop(self):
        self.logger.info(f"Watcher stopping on {self.path}/*{self.pattern}...")

        self.watching = False

        if self.recursive:
            self.i = None
        else:
            self.i.remove_watch(self.path)

    def do_watch_loop(self):
        # If we're in NEW or RENAME mode, then scan the folder once we have enqueued any waiting items
        if self.mode == mwax_mover.MODE_WATCH_DIR_FOR_NEW or self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME:
            utils.scan_for_existing_files(self.logger, self.path, self.pattern, self.recursive, self.q)

        while self.watching:
            for event in self.i.event_gen(timeout_s=0.1, yield_nones=False):
                (header, types, path, filename) = event

                # check event is one we care about
                if header.mask | self.mask == self.mask:
                    # Check file extension is one we care about
                    if os.path.splitext(filename)[1] == self.pattern or self.pattern == ".*":
                        dest_filename = os.path.join(path, filename)
                        self.q.put(dest_filename)
                        self.logger.info(f'{dest_filename} added to queue ({self.q.qsize()})')

    def get_status(self) -> dict:
        total_bytes, used_bytes, free_bytes = utils.get_disk_space_bytes(self.path)

        return {"watching": self.watching,
                "mode": self.mode,
                "watch_path": self.path,
                "watch_pattern": self.pattern,
                "watch_used_bytes": used_bytes,
                "watch_free_bytes": free_bytes}
