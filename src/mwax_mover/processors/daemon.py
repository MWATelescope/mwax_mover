"""Shared lifecycle base class for the mwax_mover long-running daemons.

This is a different kind of thing from the rest of `processors/`: the other
modules here are per-file MWAXWatchQueueWorker subclasses with a uniform
`handler(self, item: str) -> bool` contract, while MWAXDaemon is the shared
lifecycle base class for the four top-level CLI daemon classes
(MWACacheArchiveProcessor, MWAXCalvinController, MWAXCalvinProcessor,
MWAXSubfileDistributor). The two concepts are unrelated beyond both living
under `processors/` -- see docs/CLEANUP.md 6.3 for why this module lives
here rather than under `cli/` or `core/`.
"""

import argparse
import json
import logging
import sys
import time
from abc import ABC, abstractmethod
from configparser import ConfigParser

from mwax_mover import version
from mwax_mover.constants import SECTION_MWAX_MOVER
from mwax_mover.core.config import read_config
from mwax_mover.net.multicast import get_ip_address, send_multicast

logger = logging.getLogger(__name__)

# Interval, in seconds, that sleep() breaks a long wait into so it stays
# responsive to self.running being cleared mid-wait.
SECS_PER_INTERVAL = 5


class MWAXDaemon(ABC):
    """Shared lifecycle for the four long-running mwax_mover daemons.

    Provides the methods and attributes common to MWACacheArchiveProcessor,
    MWAXCalvinController, MWAXCalvinProcessor and MWAXSubfileDistributor:
    fatal-shutdown signalling, the health-multicast loop, an interruptible
    sleep, signal handling, status reporting, and command-line
    initialisation. Where a daemon's behaviour genuinely differs, this class
    exposes a hook (with a no-op default) for the subclass to override, or
    leaves the method abstract for the subclass to implement outright.
    """

    #: Subclass sets this; used by initialise_from_command_line() as the
    #: argument parser's description text.
    DESCRIPTION: str = ""

    def __init__(self) -> None:
        """Initialise the attributes every daemon shares."""
        self.running: bool = False
        self.hostname: str = ""
        self.fatal_exit_code: int = 0
        self.fatal_reason: str = ""

        # Health config. All four are read directly from the [mwax mover]
        # section except health_multicast_interface_ip, which is derived at
        # runtime from cfg_health_multicast_interface_name -- deliberately
        # no cfg_ prefix, see docs/CLEANUP.md 5.1. Do not "fix" that
        # inconsistency.
        self.cfg_health_multicast_interface_name: str = ""
        self.cfg_health_multicast_ip: str = ""
        self.cfg_health_multicast_port: int = 0
        self.cfg_health_multicast_hops: int = 0
        self.health_multicast_interface_ip: str = ""

    # --- concrete, shared ---

    def request_fatal_shutdown(self, exit_code: int, reason: str) -> None:
        """Ask the main thread to shut the whole daemon down and exit non-zero.

        Worker threads cannot terminate the process themselves: sys.exit() on a
        non-main thread raises SystemExit in that thread only, which kills the
        thread and discards the exit code, leaving the daemon running with one
        fewer worker. Worker code that hits an unrecoverable error should call
        this instead, then stop what it is doing.

        The first caller wins, so the exit code reflects the original cause
        rather than any knock-on failure. Safe to call more than once and from
        any thread.

        Args:
            exit_code: Non-zero process exit code for main() to exit with.
            reason: Human-readable description, logged and included in the
                final shutdown message.
        """
        if self.fatal_exit_code:
            # Already shutting down for an earlier (root cause) reason.
            logger.warning(f"Additional fatal error while shutting down: {reason}")
            return

        logger.error(f"FATAL: {reason} Requesting shutdown with exit code {exit_code}.")
        self.fatal_exit_code = exit_code
        self.fatal_reason = reason
        self.running = False

    def _read_health_config(self, config: ConfigParser) -> None:
        """Read the [mwax mover] health-multicast config used by health_loop().

        Sets the four cfg_health_multicast_* attributes plus the derived
        health_multicast_interface_ip (see docs/CLEANUP.md 5.1) -- kept
        together since the derivation depends on one of the reads. Call
        this from initialise().

        Args:
            config: A ConfigParser instance with the configuration already loaded.
        """
        self.cfg_health_multicast_interface_name = read_config(
            config, SECTION_MWAX_MOVER, "health_multicast_interface_name"
        )
        self.cfg_health_multicast_ip = read_config(config, SECTION_MWAX_MOVER, "health_multicast_ip")
        self.cfg_health_multicast_port = int(read_config(config, SECTION_MWAX_MOVER, "health_multicast_port"))
        self.cfg_health_multicast_hops = int(read_config(config, SECTION_MWAX_MOVER, "health_multicast_hops"))

        # get this hosts primary network interface ip
        # Deliberately no cfg_ prefix: this is derived at runtime from
        # cfg_health_multicast_interface_name, not read directly from config
        # (see docs/CLEANUP.md 5.1). Do not "fix" this inconsistency.
        self.health_multicast_interface_ip = get_ip_address(self.cfg_health_multicast_interface_name)
        logger.info(f"IP for sending multicast: {self.health_multicast_interface_ip}")

    def health_loop(self) -> None:
        """Periodically send health status via UDP multicast.

        Runs in a separate thread and sends status information every second
        while the daemon is running.
        """
        while self.running:
            self.before_health_send()

            status_dict = self.get_status()
            status_bytes = json.dumps(status_dict).encode("utf-8")

            try:
                send_multicast(
                    self.health_multicast_interface_ip,
                    self.cfg_health_multicast_ip,
                    self.cfg_health_multicast_port,
                    status_bytes,
                    self.cfg_health_multicast_hops,
                )
            except Exception:
                logger.exception("health_loop: Failed to send health information. Ignoring and continuing")

            self.sleep(1)

    def sleep(self, seconds: float) -> None:
        """Sleep for a specified duration while remaining responsive to shutdown.

        Breaks long sleeps into intervals to remain responsive to the running
        flag and shutdown directives.

        Args:
            seconds: Duration to sleep in seconds.
        """
        if not self.running:
            return

        if seconds <= SECS_PER_INTERVAL:
            time.sleep(seconds)
            return

        integer_intervals, remainder_secs = divmod(seconds, SECS_PER_INTERVAL)

        while self.running and integer_intervals > 0:
            time.sleep(SECS_PER_INTERVAL)
            integer_intervals -= 1
            self.during_sleep_interval()

        if self.running and remainder_secs > 0:
            time.sleep(remainder_secs)

    def signal_handler(self, _signum, _frame) -> None:
        """Handle SIGINT and SIGTERM signals for graceful shutdown.

        Args:
            _signum: Signal number (unused).
            _frame: Stack frame (unused).
        """
        logger.warning(f"Interrupted. Shutting down{self.shutdown_log_detail()}...")
        self.stop()

    def get_status(self) -> dict:
        """Return this daemon's status as a dictionary, for the health multicast.

        Returns:
            {"main": {...}} where the inner dict holds the five keys every
            daemon reports (unix_timestamp, process, version, host, running)
            plus cmdline, merged with get_extra_status()'s daemon-specific
            keys. A top-level "workers" key is added only when
            get_worker_status() returns a list rather than None.
        """
        main_status = {
            "unix_timestamp": time.time(),
            "process": type(self).__name__,
            "version": version.get_mwax_mover_version_string(),
            "host": self.hostname,
            "running": self.running,
            "cmdline": " ".join(sys.argv[1:]),
        }
        main_status.update(self.get_extra_status())

        status: dict = {"main": main_status}

        worker_status = self.get_worker_status()
        if worker_status is not None:
            status["workers"] = worker_status

        return status

    def initialise_from_command_line(self) -> None:
        """Initialise the daemon from command-line arguments.

        Parses command-line arguments and calls initialise() with the
        configuration file path. Uses this class's DESCRIPTION for the
        argument parser's help text.
        """
        parser = argparse.ArgumentParser()
        parser.description = self.DESCRIPTION
        parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")
        args = vars(parser.parse_args())
        config_filename = args["cfg"]
        self.initialise(config_filename)

    # --- hooks with defaults ---

    def before_health_send(self) -> None:
        """Called once per health iteration before the status is built."""
        return None

    def during_sleep_interval(self) -> None:
        """Called once per SECS_PER_INTERVAL-second interval by sleep()."""
        return None

    def get_worker_status(self) -> list[dict] | None:
        """Per-worker status for get_status()'s "workers" key.

        Returns:
            A list of per-worker status dicts, or None for daemons with no
            worker pool -- None omits the "workers" key entirely rather than
            reporting a misleading empty list.
        """
        return None

    def shutdown_log_detail(self) -> str:
        """Extra detail for the signal-handler shutdown message.

        Returns:
            Text to insert into "Interrupted. Shutting down<this>...", or an
            empty string for no extra detail.
        """
        return ""

    # --- abstract ---

    @abstractmethod
    def get_extra_status(self) -> dict:
        """Daemon-specific status keys to merge into get_status()'s result."""
        ...

    @abstractmethod
    def initialise(self, config_filename: str, *args, **kwargs) -> None:
        """Initialise the daemon from a configuration file."""
        ...

    @abstractmethod
    def start(self) -> None:
        """Start the daemon's main loop."""
        ...

    @abstractmethod
    def stop(self) -> None:
        """Stop the daemon and release its resources."""
        ...
