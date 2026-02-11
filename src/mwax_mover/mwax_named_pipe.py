import errno
import io
import logging
import os
import stat
import signal
import time
from typing import Optional

# Be explicit: ignore SIGPIPE so we get BrokenPipeError instead of process kill.
try:
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)  # no-op on Windows
except Exception:
    pass


class NamedPipeWriter:
    """
    Robust writer for a POSIX named pipe (FIFO).
    - Opens write end in non-blocking mode with retry until a reader connects.
    - Detects reader disconnects (EPIPE) and reopens.
    """

    def __init__(self, path: str, retry_delay: float = 0.2, timeout: float = 2.0):
        self.path = path
        self.retry_delay = retry_delay
        self.timeout = timeout
        self._file: Optional[io.TextIOWrapper] = None
        self._fd: Optional[int] = None
        self.logger = logging.getLogger(__name__)

    def open(self) -> None:
        """Open the FIFO for writing, retrying until at least one reader is present."""
        self.close()

        start_time = time.time()

        # Keep trying until a reader connects or we timeout
        while time.time() - start_time <= self.timeout:
            try:
                fd = os.open(self.path, os.O_WRONLY | os.O_NONBLOCK)
                self._fd = fd
                raw = io.FileIO(self._fd, mode="w", closefd=False)
                buffered = io.BufferedWriter(raw, 1)
                self._file = io.TextIOWrapper(buffered, encoding="utf-8", write_through=True)
                return  # Successfully opened
            except OSError as e:
                if e.errno in (errno.ENXIO, errno.ENOENT):  # no reader yet or pipe disappeared
                    time.sleep(self.retry_delay)
                    # If pipe was removed, wait for it to reappear
                    try:
                        st = os.stat(self.path)
                        if not stat.S_ISFIFO(st.st_mode):
                            raise RuntimeError(f"{self.path} is not a named pipe")
                    except FileNotFoundError:
                        pass
                    continue
                raise

        # If we got here we hit the timeout
        raise TimeoutError(f"Timeout waiting for reader to connect to named pipe {self.path}")

    def write(self, data: str) -> None:
        """
        Write data, auto-reopening if the reader disconnects
        """
        retries = 3
        retry = 0
        while retry <= retries:
            retry += 1
            try:
                if self._file is not None:
                    self._file.write(data)
                else:
                    raise BrokenPipeError
                return
            except BrokenPipeError:
                self.logger.warning(f"Named pipe {self.path} not connected (BrokenPipeError). Opening...")
                self.open()
                # loop and retry the write
            except OSError as e:
                # EPIPE is the same condition via OSError path
                if e.errno == errno.EPIPE:
                    self.logger.warning(f"Named pipe {self.path} not connected (EPIPE). Opening...")
                    self.open()
                elif e.errno in (errno.ENXIO, errno.ENOENT):
                    self.logger.warning(f"Named pipe {self.path} not connected (ENXIO / ENOENT). Opening...")
                    self.open()
                else:
                    raise  # unexpected I/O error → bubble up

    def close(self) -> None:
        try:
            self.logger.debug(f"Closing named pipe: {self.path}")
            if self._file:
                self._file.close()
        finally:
            self._file = None
            if self._fd is not None:
                try:
                    os.close(self._fd)
                except OSError:
                    pass
            self._fd = None
