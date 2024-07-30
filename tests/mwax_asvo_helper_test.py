import time
import json
import logging
import pytest
from mwax_mover.mwax_asvo_helper import (
    get_job_id_from_giant_squid_stdout,
    get_job_info_from_giant_squid_json,
    MWAASVOJobState,
    MWAASVOHelper,
)


def test_get_jobid_from_giant_squid_stdout():
    with pytest.raises(Exception) as excinfo:
        get_job_id_from_giant_squid_stdout("abc123\ndef456\n")
        assert str(excinfo.value).startswith("No job_id could be found in the output from giant-squid")

    assert (
        get_job_id_from_giant_squid_stdout(
            "17:19:03 [INFO] Submitted 1234567890 as ASVO job ID 123\n"
            "17:19:03 [INFO] Submitted 1 obsids for visibility download.\n"
        )
        == 123
    )

    assert (
        get_job_id_from_giant_squid_stdout(
            "17:19:03 [INFO] Submitted 1234567890 as ASVO job ID 1234567\n"
            "17:19:03 [INFO] Submitted 1 obsids for visibility download.\n"
        )
        == 1234567
    )

    stdout = """14:34:21 [WARN] Job already queued, processing or complete. Job Id: 10001903
    14:34:21 [INFO] Submitted 0 obsids for visibility download.
    """
    assert get_job_id_from_giant_squid_stdout(stdout) == 10001903


def test_get_job_info_from_giant_squid_json_valid():
    stdout = """{
  "766223": {
    "obsid": 1396629320,
    "jobId": 766223,
    "jobType": "DownloadMetadata",
    "jobState": "Ready",
    "files": [
      {
        "jobType": "Acacia",
        "fileUrl": "https://projects.pawsey.org.au/mwa-asvo/1396629320_766223_vis_meta.tar?AWSAccessKeyId=a5e466f891734d45a67676504a309c35&Signature=VpZCOQ2exlVfIFc7nBtOqOsN8ME%3D&Expires=1715920096",
        "filePath": null,
        "fileSize": 102400,
        "fileHash": "bf64f0fcdebb37c2f8e142ed0130734b2864406f"
      }
    ]
  },
  "766227": {
    "obsid": 1290094336,
    "jobId": 766227,
    "jobType": "DownloadVoltage",
    "jobState": {
      "Error": "No files found."
    },
    "files": null
  }
 }"""
    json_stdout = json.loads(stdout)

    i = 1
    for json_one_job in json_stdout:
        job_id = None
        job_state = None
        url = None

        obs_id, job_id, job_state, url = get_job_info_from_giant_squid_json(json_stdout, json_one_job)

        match i:
            case 1:
                assert obs_id == 1396629320
                assert job_id == 766223
                assert job_state == MWAASVOJobState.Ready
                assert (
                    url
                    == "https://projects.pawsey.org.au/mwa-asvo/1396629320_766223_vis_meta.tar?AWSAccessKeyId=a5e466f891734d45a67676504a309c35&Signature=VpZCOQ2exlVfIFc7nBtOqOsN8ME%3D&Expires=1715920096"
                )
            case 2:
                assert obs_id == 1290094336
                assert job_id == 766227
                assert job_state == MWAASVOJobState.Error
                assert url is None
        i += 1


def test_get_status_from_giant_squid_stdout_invalid():
    # Test for unknown error code
    stdout = """{
  "766227": {
    "obsid": 1290094336,
    "jobId": 766227,
    "jobType": "DownloadVoltage",
    "jobState": {
      "UnhandledErrorCode": "This shouldnt happen"
    },
    "files": null
  }
 }"""
    json_stdout = json.loads(stdout)

    for json_one_job in json_stdout:
        with pytest.raises(Exception) as excinfo:
            obs_id, job_id, job_state, url = get_job_info_from_giant_squid_json(json_stdout, json_one_job)

            assert str(excinfo.value) == ("766227: giant-squid unknown job status code UnhandledErrorCode.")


def test_mwax_asvo_helper():
    asvo = MWAASVOHelper()
    logger = logging.getLogger(__name__)

    asvo.initialise(
        logger,
        "../giant-squid/target/release/giant-squid",
        10,
        10,
        3600,
        "/tmp",
    )
    asvo.submit_download_job(1354865168)

    running = True

    while running:
        asvo.update_all_job_status(False)

        for job in asvo.current_asvo_jobs:
            if job.obs_id == 1354865168 and job.job_state == MWAASVOJobState.Ready:
                asvo.download_asvo_job(job)

                # Remove it from the list
                asvo.current_asvo_jobs.remove(job)
                running = False

        time.sleep(10)

    assert len(asvo.current_asvo_jobs) == 0
