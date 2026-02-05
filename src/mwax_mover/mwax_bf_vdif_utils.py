from typing import List
import logging
from mwalib import MetafitsContext
import mwax_mover.version
import re
import shutil


class VDIFHeader:
    def __init__(self):
        self.VDIF_HDR_VERSION: str = "0.2"
        self.MWA_CAPTURE_VERSION: str = mwax_mover.version.get_mwax_mover_version_string()
        self.MWA_SAMPLE_VERSION: str = "0.1"
        self.TELESCOPE: str = "MWA"
        self.MODE: str = "MWAX_BEAMFORMER"
        self.INSTRUMENT: str = "VDIF"
        self.NPOL: int = 2
        self.NBIT: int = 8
        self.NDIM: int = 2

        self.mwax_beamfomer_version: str = "0.1"
        self.datafile: str = ""
        self.mjd_start: float = 0.0
        self.mjd_epoch: float = 0.0
        self.sec_offset: float = 0.0
        self.source: str = ""
        self.ra: str = ""
        self.dec: str = ""
        self.freq: float = 0.0
        self.bw: float = 0.0
        self.tsamp: float = 0.0

    def populate(self, metafits_filename: str):
        mc = MetafitsContext(metafits_filename)

        self.mjd_start = mc.sched_start_mjd
        self.mjd_epoch = mc.sched_start_mjd
        self.sec_offset = 0
        self.source = mc.obs_name
        self.ra = str(0)  # TODO: get from voltage beams info from mwalib new version
        self.dec = str(0)  # TODO: get from voltage beams info from mwalib new version
        self.freq = mc.centre_freq_hz / 1000000.0  # convert Hz to MHz
        self.bw = mc.obs_bandwidth_hz / 1000000.0  # convert Hz to MHz
        self.tsamp = 0.781  # TODO: get from voltage beams info from mwalib new version

    def write(self, vdif_hdr_filename):
        """
        Write an ASCII header file using the fields stored
        """

        lines = [
            f"HDR_VERSION {self.VDIF_HDR_VERSION}                   # Version of this ASCII header",
            f"MWA_CAPTURE_VERSION {self.MWA_CAPTURE_VERSION}        # Version of the Data Acquisition Software",
            f"MWA_SAMPLE_VERSION {self.MWA_SAMPLE_VERSION}          # Version of the FFD FPGA Software",
            f"MWAX_BEAMFORMER_VERSION {self.mwax_beamfomer_version} # Version of the MWAX Beamformer Software",
            "",
            f"TELESCOPE    {self.TELESCOPE}  # telescope name",
            f"MODE         {self.MODE}       # observing mode",
            f"INSTRUMENT   {self.INSTRUMENT} # instrument name",
            f"DATAFILE     {self.datafile}   # raw data file name",
            "",
            f"MJD_START    {self.mjd_start}  # MJD of the start of the observation",
            f"MJD_EPOCH    {self.mjd_epoch}  # MJD of the data epoch",
            f"SEC_OFFSET   {self.sec_offset} # seconds offset from the start of the observation",
            "",
            f"SOURCE       {self.source} # name of the astronomical source",
            f"RA           {self.ra}     # Right Ascension of the source",
            f"DEC          {self.dec}    # Declination of the source",
            "",
            f"FREQ         {self.freq}  # centre frequency on sky in MHz",
            f"BW           {self.bw}    # bandwidth in MHz (-ve lower sb)",
            f"TSAMP        {self.tsamp} # sampling interval in microseconds",
            "",
            f"NBIT         {self.NBIT} # number of bits per sample",
            f"NDIM         {self.NDIM} # dimension of samples (2=complex, 1=real)",
            f"NPOL         {self.NPOL} # number of polarisations observed",
            "",
        ]

        # Write the header
        with open(vdif_hdr_filename, "w") as f:
            for line in lines:
                f.write(line + "\n")


def get_stitched_filename(filename: str) -> str:
    """
    Convert 'obsid_subobs_chXXX_beamNN.vdif'
    into    'obsid_chXXX_beamNN.vdif'.

    obsid  = 10 digits
    subobs = 10 digits
    XXX    = 3 digits (zero padded)
    NN     = 2 digits (zero padded)
    """
    pattern = r"^(?P<obsid>\d{10})_(?P<subobs>\d{10})_ch(?P<chan>\d{3})_beam(?P<beam>\d{2})\.vdif$"
    m = re.match(pattern, filename)

    if not m:
        raise ValueError(f"Filename does not match expected format: {filename}")

    obsid = m.group("obsid")
    chan = m.group("chan")
    beam = m.group("beam")

    return f"{obsid}_ch{chan}_beam{beam}.vdif"


def stitch_vdif_files_and_write_hdr(
    logger: logging.Logger,
    metafits_filename: str,
    files: List[str],
    output_vdif_filename: str,
    output_hdr_filename: str,
):
    if len(files) == 0:
        raise Exception("No VDIF files to stitch")

    if len(files) == 1:
        # Nothing to stitch- but we still need the output_vdif_filename to be created, so copy the file
        logger.debug(f"Only one VDIF file, no stiching needed: copying {files[0]} to {output_vdif_filename}")
        shutil.copyfile(files[0], output_vdif_filename)
    else:
        # The filenames will ensure a good sort order
        sorted_files = sorted(files)

        logger.info(f"Stitching {len(sorted_files)} VDIF files: {sorted_files[0]}...{sorted_files[-1]}")

        with open(output_vdif_filename, "wb") as output:
            for f in sorted_files:
                with open(f, "rb") as input_file:
                    while True:
                        chunk = input_file.read(1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)

        logger.info(f"Successfully stitched VDIF files into {output_vdif_filename}")

    # Write the header file
    hdr = VDIFHeader()
    hdr.populate(metafits_filename)
    hdr.write(output_hdr_filename)

    logger.info(f"Successfully wrote VDIF header file into {output_hdr_filename}")
