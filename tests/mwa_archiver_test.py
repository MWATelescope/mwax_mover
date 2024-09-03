"""Tests for the mwa archving primitives"""

import logging
import boto3
from mwax_mover.mwa_archiver import ceph_get_s3_session, ceph_get_s3_resource


def test_ceph_get_s3_session():
    """Test that we get an s3 session correctly"""
    session: boto3.Session = ceph_get_s3_session("acacia_test")

    assert session is not None
    assert session.profile_name == "acacia_test"


def test_ceph_get_s3_resource():
    """Test that we can get a boto3 resource representing our endpoint"""
    logger = logging.getLogger("test")

    session: boto3.Session = ceph_get_s3_session("acacia_test")
    resource: boto3.session.Session.resource = ceph_get_s3_resource(logger, session, ["https://projects.pawsey.org.au"])

    assert resource is not None
