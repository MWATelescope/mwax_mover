import os
import shutil
import signal
import threading
import time
from mwax_mover.mwax_calvin_download_processor import MWAXCalvinDownloadProcessor


def test_calvin_downloader_test():
    # Set up MWAXCalvinDownloaderProcessor instance
    test_dir = "tests/mock_mwax_calvin_download_test"

    # Clear/create data path
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.mkdir(test_dir)

    processor: MWAXCalvinDownloadProcessor = MWAXCalvinDownloadProcessor()

    processor.initialise("tests/calvin_download_processor.cfg")

    # Start the pipeline
    # Create and start a thread for the processor
    thrd = threading.Thread(name="caldl_main_thread", target=processor.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(5)

    # Quit
    # Ok time's up! Stop the processor
    processor.signal_handler(signal.SIGINT, 0)
    thrd.join()