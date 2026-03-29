"""Utilities for reading, constructing, and stitching VDIF beamformer output files.

Provides the VDIFHeader class, which populates VDIF header metadata (pointing,
frequency, timing) from a metafits file, and functions to concatenate multiple
per-subobservation VDIF files produced by the MWAX beamformer into a single
complete observation output file.
"""

from typing import List
import logging
from astropy.io import fits
from mwalib import MetafitsContext
import mwax_mover.version
import os
import re
import shutil

logger = logging.getLogger(__name__)


class VDIFHeader:
    def __init__(self):
        """Initialize a VDIF header with default values and metadata.

        Sets up all VDIF header fields with default values for telescope,
        mode, instrument, and data format specifications.
        """
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

    def populate(self, metafits_filename: str, rec_chan: int, beam_no: int):
        """Populate VDIF header fields from a metafits file.

        Reads observation metadata including timing, pointing, frequency,
        source name, and beam-specific information from the metafits file
        and stores them in the header fields.

        Args:
            metafits_filename: Path to the metafits file.
            rec_chan: Receiver channel number.
            beam_no: Beam number index.
        """
        mc = MetafitsContext(metafits_filename)
        self.mjd_start = mc.sched_start_mjd
        self.mjd_epoch = mc.sched_start_mjd
        self.sec_offset = 0

        self.source = mc.obs_name  # might get overridden by metafits so this is the default
        self.ra = str(0)  # might get overridden by metafits so this is the default
        self.dec = str(0)  # might get overridden by metafits so this is the default
        self.tsamp = 1.0 / 1280000  # might get overridden by metafits so this is the default

        if mc.metafits_voltage_beams:
            self.voltage_beam = mc.metafits_voltage_beams[beam_no]
            self.ra = str(self.voltage_beam.ra_deg)
            self.dec = str(self.voltage_beam.dec_deg)
            self.tsamp = 1.0 / self.voltage_beam.frequency_resolution_hz

        # Find the coarse channel
        for chan in mc.metafits_coarse_chans:
            if chan.rec_chan_number == rec_chan:
                self.bw = chan.chan_width_hz / 1000000.0  # convert Hz to MHz
                self.freq = chan.chan_centre_hz / 1000000.0  # convert Hz to MHz
                break

        # There are some optional values in newer metafits which are not yet supported in mwalib
        with fits.open(metafits_filename) as hdul:
            BEAM_ALT_AZ_HDU = "BEAMALTAZ"

            # Get the start RA and DEC
            try:
                beamaltaz_hdu = hdul[BEAM_ALT_AZ_HDU]
                s_ra = beamaltaz_hdu.header[f"B{beam_no:02d}_SRA"]
                s_dec = beamaltaz_hdu.header[f"B{beam_no:02d}_SDEC"]

                if s_ra is not None:
                    self.ra = s_ra
                if s_dec is not None:
                    self.dec = s_dec
            except KeyError:
                logger.warning(f"Unable to read {BEAM_ALT_AZ_HDU} values")
                pass

            except Exception:
                logger.exception(f"Unable to read {BEAM_ALT_AZ_HDU} values")
                pass

            # Get the beam target name
            VOLTAGE_BEAMS_HDU = "VOLTAGEBEAMS"
            try:
                voltagebeams_hdu = hdul[VOLTAGE_BEAMS_HDU]
                target_name = voltagebeams_hdu.data["target_name"][beam_no].strip()

                # Did we get a value?s
                if target_name is not None:
                    # Check for empty string
                    if target_name != "":
                        self.source = target_name

            except KeyError:
                logger.warning(f"Unable to read {VOLTAGE_BEAMS_HDU} values")
                pass

            except Exception:
                logger.exception(f"Unable to read {VOLTAGE_BEAMS_HDU} values")
                pass

    def write(self, vdif_hdr_filename: str):
        """Write VDIF header fields to an ASCII header file.

        Generates and writes all header metadata fields to the specified
        file in a human-readable ASCII format with comments.

        Args:
            vdif_hdr_filename: Path where the header file will be written.
        """
        self.datafile = os.path.basename(vdif_hdr_filename.replace(".hdr", ".vdif"))

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


def get_vdif_filename_components(filename: str) -> tuple[str, int, int, int, int]:
    """Parse filename components from a per-subobservation VDIF file path.

    Extracts path, observation ID, sub-observation ID, receiver channel,
    and beam from a filename matching: /path/obsid_subobs_chXXX_beamNN.vdif

    Args:
        filename: The VDIF filename to parse.

    Returns:
        A tuple of (path: str, obsid: int, subobs: int, rec_chan: int, beam: int).

    Raises:
        ValueError: If filename does not match the expected format.
    """
    pattern = r"^(?P<path>.*)/(?P<obsid>\d{10})_(?P<subobs>\d{10})_ch(?P<chan>\d{3})_beam(?P<beam>\d{2})\.vdif$"
    m = re.match(pattern, filename)

    if not m:
        raise ValueError(f"Filename does not match expected format: {filename}")

    file_path = str(m.group("path"))
    obsid = int(m.group("obsid"))
    subobsid = int(m.group("subobs"))
    chan = int(m.group("chan"))
    beam = int(m.group("beam"))

    return file_path, obsid, subobsid, chan, beam


def get_stitched_filename(filename: str) -> str:
    """Generate output filename by removing sub-observation ID from input filename.

    Converts a per-subobservation filename to a complete observation filename:
    obsid_subobs_chXXX_beamNN.vdif -> obsid_chXXX_beamNN.vdif

    Args:
        filename: The per-subobservation VDIF filename.

    Returns:
        The output filename path with sub-observation ID removed.

    Raises:
        ValueError: If filename does not match the expected format.
    """
    file_path, obsid, _, chan, beam = get_vdif_filename_components(filename)

    return os.path.join(file_path, f"{obsid}_ch{chan:03d}_beam{beam:02d}.vdif")


def stitch_vdif_files_and_write_hdr(metafits_filename: str, files: List[str], output_dir: str) -> tuple[str, str]:
    """Concatenate VDIF files and generate a header file with beam metadata.

    Combines per-subobservation VDIF files produced by the MWAX beamformer
    into a single complete observation file, then creates an ASCII header
    file with all metadata extracted from the metafits file.

    Args:
        metafits_filename: Path to the metafits file for metadata.
        files: List of VDIF file paths to stitch together.
        output_dir: Directory where output VDIF and header files will be written.

    Returns:
        A tuple of (output_vdif_filename: str, output_hdr_filename: str).

    Raises:
        Exception: If the files list is empty.
    """
    if len(files) == 0:
        raise Exception("No VDIF files to stitch")

    output_vdif_filename: str = get_stitched_filename(files[0])
    output_vdif_filename = os.path.join(output_dir, os.path.basename(output_vdif_filename))
    output_hdr_filename: str = output_vdif_filename.replace(".vdif", ".hdr")

    if len(files) == 1:
        # Nothing to stitch- but we still need the output_vdif_filename to be created, so copy the file
        logger.debug(f"Only one VDIF file, no stiching needed: copying {files[0]} to {output_vdif_filename}")
        shutil.copyfile(files[0], output_vdif_filename)
    else:
        # The filenames will ensure a good sort order
        sorted_files = sorted(files)

        logger.info(f"Stitching {len(sorted_files)} VDIF files: {sorted_files}")

        with open(output_vdif_filename, "wb") as output:
            for f in sorted_files:
                with open(f, "rb") as input_file:
                    while True:
                        chunk = input_file.read(1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)

        logger.info(f"Successfully stitched VDIF files into {output_vdif_filename}")

    logger.info(f"Writing VDIF header file into {output_hdr_filename}...")

    # get the rec chan number
    _, _, _, rec_chan, beam_no = get_vdif_filename_components(files[0])

    # Write the header file
    hdr = VDIFHeader()
    hdr.populate(metafits_filename, rec_chan, beam_no)
    hdr.write(output_hdr_filename)

    logger.info(f"Successfully wrote VDIF header file into {output_hdr_filename}")
    return output_vdif_filename, output_hdr_filename
