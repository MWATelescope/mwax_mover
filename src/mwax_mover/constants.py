"""Module-level constants shared across the mwax_mover package.

Command-string substitution tokens: __FILE__/__FILENOEXT__
(FILE_REPLACEMENT_TOKEN/FILENOEXT_REPLACEMENT_TOKEN).

Directory watch modes: MODE_WATCH_DIR_FOR_NEW/_FOR_RENAME/_FOR_RENAME_OR_NEW.

INI config: the section names used across the CLI daemons (SECTION_*,
except the three metadata-database sections -- see the comment above
SECTION_MWAX_MOVER); the CONFIG_KEY_* names for config keys read
identically by two or more CLI daemons.

Time/size/exit-code basics: SECONDS_PER_MINUTE/SECONDS_PER_HOUR;
EXIT_FAILURE, the single non-zero process exit code used for every
abnormal CLI termination; DEFAULT_POSTGRES_PORT.

Network: MWA_WEBSERVICE_HOSTS, the two MWA webservice hosts tried in
order by every caller that queries the metadata webservice.

File naming: the MWA data file extensions (EXT_FITS, EXT_SUB, EXT_VDIF,
EXT_FIL, EXT_HDR, EXT_UVFITS); the hyperdrive solution-file naming
convention (SOLUTIONS_FITS_SUFFIX and friends); INDEX_JSON_FILENAME, the
calibration-fit index manifest filename; COMMAND_DADA_DISKDB, the
external binary name used to load a subfile into a PSRDADA ring buffer.

Shared daemon behaviour: LOG_FORMAT, the shared logging.Formatter format
string for the CLI daemons/scripts; HEALTH_THREAD_NAME, the
health-reporting thread name shared by the four CLI daemons;
DUMMY_CONFIG_VALUE, the "no real database configured" sentinel;
METAFITS_KEY_EXPOSURE, the FITS/metafits exposure-duration header key.

Calibration numerics: MAD_TO_STD_SCALE_FACTOR, the MAD-to-standard-
deviation conversion used by the outlier-rejection code; the di-calibrate
memory-estimate constants (JONES_F32_BYTES, JONES_F64_BYTES, F32_BYTES,
F64_BYTES); HYPERDRIVE_MEMORY_HEADROOM_FRACTION and
HYPERDRIVE_FALLBACK_WORKERS, used to size concurrent hyperdrive runs
across picket-fence bands against host RAM; the equivalent GPU-side
constants (NVIDIA_A40_VRAM_BYTES, HYPERDRIVE_GPU_RESIDENT_FIXED_BYTES,
HYPERDRIVE_MAX_CONCURRENT_GPU_WORKERS, HYPERDRIVE_GPU_LMN_BYTES,
HYPERDRIVE_GPU_GAUSSIAN_PARAMS_BYTES), used to additionally cap that same
sizing against the shared GPU's memory and measured compute-contention
ceiling; the REFTILE_* gates used by select_refant to choose a
calibration-quality-aware reference tile; and MWA_NUM_COARSE_CHANS, the
standard MWA coarse-channel count.
"""

# The full filename with path
FILE_REPLACEMENT_TOKEN = "__FILE__"

# Full filename/path but with no extension
FILENOEXT_REPLACEMENT_TOKEN = "__FILENOEXT__"

MODE_WATCH_DIR_FOR_RENAME = "WATCH_DIR_FOR_RENAME"
MODE_WATCH_DIR_FOR_NEW = "WATCH_DIR_FOR_NEW"
MODE_WATCH_DIR_FOR_RENAME_OR_NEW = "WATCH_DIR_FOR_RENAME_OR_NEW"

# INI config section names. The three metadata-database sections (mro/remote/
# mwa metadata database) are deliberately not included here -- they are
# consolidated into a single [mwa database] section in a later cleanup phase.
SECTION_MWAX_MOVER = "mwax mover"
SECTION_CORRELATOR = "correlator"
SECTION_BEAMFORMER = "beamformer"
SECTION_CALVIN = "calvin"
SECTION_BIRLI = "birli"
SECTION_HYPERDRIVE = "hyperdrive"
SECTION_GIANT_SQUID = "giant squid"
SECTION_PLOTS_UPLOAD = "plots upload"
SECTION_DOWNLOADING = "downloading"
SECTION_PROCESSING = "processing"
SECTION_ARCHIVING = "archiving"

# The single consolidated metadata-database section. Previously three
# separate sections (mwa/mro/remote metadata database) all pointing at the
# same database -- see docs/CLEANUP.md 5.2.
SECTION_MWA_DATABASE = "mwa database"

# The two MWA webservice hosts, tried in order (MRO-local first, then
# public) by every caller that queries the metadata webservice. Identical
# in every deployment, so these are constants rather than config.
MWA_WEBSERVICE_HOSTS = ("http://mro.mwa128t.org", "http://ws.mwatelescope.org")

SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600

# Process exit code for any abnormal termination. The specific value carries
# no meaning to any caller; only zero vs non-zero is significant.
EXIT_FAILURE = 1

# Scale factor converting a median absolute deviation (MAD) to an equivalent
# standard deviation for a normal distribution (1 / Phi^-1(3/4)), used
# wherever a MAD-based robust threshold needs to be comparable to a
# mean+nstd*std threshold. See docs/CONSTANTS_CLEANUP.md 1.1.
MAD_TO_STD_SCALE_FACTOR = 1.4826

# Sigma multipliers defining a "good" channel in the calibration fits: the
# fraction of channels within this many (robust) standard deviations of the
# fitted line is the reported fit quality. Both are 2.0 today.
#
# PHASE_FIT_CLIP_SIGMA does double duty in fit_phase_line: it is the iterative
# sigma-clip threshold used *during* the fit (how aggressively outliers are
# rejected each pass), and because phase quality is the fraction of channels
# surviving that clip, it also defines phase quality. GAIN_QUALITY_SIGMA, by
# contrast, is a post-fit ratio only (fraction within N-sigma of the gain fit),
# with no effect on the fit itself -- hence two separate constants.
#
# NOTE: the MWA ASVO calibration-database pipeline (Sokolowski et al. 2020)
# defines its good-channel quality ratio at 5 sigma, not 2. These are the knobs
# to revisit if aligning Calvin's quality metric with that convention.
PHASE_FIT_CLIP_SIGMA = 2.0
GAIN_QUALITY_SIGMA = 2.0

# Standard number of MWA coarse channels.
MWA_NUM_COARSE_CHANS = 24

# Shared logging.Formatter format string for the CLI daemons/scripts. See
# docs/CONSTANTS_CLEANUP.md 1.2.
LOG_FORMAT = "%(asctime)s, %(levelname)s, %(name)s.%(funcName)s, %(message)s"

# MWA data file extensions, matched throughout the ingest/archive/beamformer
# pipeline. See docs/CONSTANTS_CLEANUP.md 1.3.
EXT_FITS = ".fits"
EXT_SUB = ".sub"
EXT_VDIF = ".vdif"
EXT_FIL = ".fil"
EXT_HDR = ".hdr"

# Hyperdrive solution-file naming convention, matched across the calvin
# pipeline (processing, upload staging, plot indexing). See
# docs/CONSTANTS_CLEANUP.md 1.4.
SOLUTIONS_FITS_SUFFIX = "solutions.fits"
SOLUTIONS_ORIGINAL_FITS_SUFFIX = "solutions.original.fits"
SOLUTIONS_FITS_GLOB = "*_solutions.fits"
SOLUTIONS_ORIGINAL_FITS_GLOB = "*_solutions.original.fits"

# The calibration-fit index manifest filename, written by
# calvin.plots.index.generate_plot_index_file and read/uploaded by several
# CLI scripts.
INDEX_JSON_FILENAME = "index.json"

# Thread name used for the health-reporting thread in all four CLI daemons.
HEALTH_THREAD_NAME = "health_thread"

# Sentinel config value meaning "no real database configured" -- checked
# wherever a daemon decides whether to actually connect/decode credentials.
DUMMY_CONFIG_VALUE = "dummy"

# Default Postgres port, used as the pre-config-read default for
# self.cfg_db_port in the two daemons that declare one.
DEFAULT_POSTGRES_PORT = 5432

# FITS/metafits header key for exposure duration, used when determining a
# beamformer observation's last expected subobs. See
# docs/CONSTANTS_CLEANUP.md 1.6.
METAFITS_KEY_EXPOSURE = "EXPOSURE"

# Config key names read identically by two or more of the four CLI daemons,
# with the key string typed out separately at each site. See
# docs/CONSTANTS_CLEANUP.md 1.7.
CONFIG_KEY_LOG_LEVEL = "log_level"
CONFIG_KEY_GIANT_SQUID_BINARY_PATH = "giant_squid_binary_path"
CONFIG_KEY_HIGH_PRIORITY_CORRELATOR_PROJECTIDS = "high_priority_correlator_projectids"
CONFIG_KEY_HIGH_PRIORITY_VCS_PROJECTIDS = "high_priority_vcs_projectids"
CONFIG_KEY_ARCHIVE_COMMAND_TIMEOUT_SEC = "archive_command_timeout_sec"

# The dada_diskdb binary name, used to build the command line that loads a
# subfile into a PSRDADA ring buffer. See docs/CONSTANTS_CLEANUP.md 3.3 --
# previously an unused local constant in processors/subfile_incoming.py
# while fits/subfile.py independently hardcoded the same name.
COMMAND_DADA_DISKDB = "dada_diskdb"

# Bytes per Jones matrix element in di-calibrate's working arrays -- a
# Jones<f32> is 4 complex numbers at 4 bytes each (real+imag), Jones<f64>
# the same at 8 bytes each. Used by estimate_di_calibrate_peak_ram_bytes.
# See estimate_di_calibrate_peak_ram_bytes in calvin/hyperdrive.py.
JONES_F32_BYTES = 32
JONES_F64_BYTES = 64
F32_BYTES = 4
F64_BYTES = 8

# UVFITS output extension, produced by Birli and consumed by hyperdrive.
EXT_UVFITS = ".uvfits"

# Fraction of live-probed available memory reserved as headroom when
# sizing concurrent hyperdrive runs -- leaves room for the parent process
# and anything else sharing the allocation. Tune here if picket-fence
# runs are still getting OOM-killed or are leaving memory idle. See
# See _max_hyperdrive_workers in calvin/hyperdrive.py.
HYPERDRIVE_MEMORY_HEADROOM_FRACTION = 0.15

# Fallback worker count when available memory can't be determined at all.
# Deliberately 1 (fully serial) -- unlike the plotting pool's fallback of
# 4 (see calvin.plots.gains._PAGE_RENDER_FALLBACK_WORKERS), a failed
# hyperdrive run is a failed calibration, not just a slow plot, so
# guessing low here costs more to get wrong.
HYPERDRIVE_FALLBACK_WORKERS = 1

# NVIDIA A40 GPU memory -- Calvin's hardware, confirmed via NVIDIA's own
# product spec (48 GB GDDR6 with ECC). Used by _max_hyperdrive_workers to
# cap concurrent hyperdrive runs against the single GPU all pickets within
# one SLURM job share (see calvin/slurm.py's --gpus-per-task=1).
NVIDIA_A40_VRAM_BYTES = 48 * 1024**3

# Empirically measured (nvidia-smi) on Calvin hardware, for a cuda-only
# (double precision, no gpu-single) hyperdrive build: CUDA context overhead
# (~271 MiB) plus mwa_hyperbeam's FEE beam-coefficient upload (~2634 MiB).
# Measured flat across 1 vs 24 coarse channels and at --num-sources 200, so
# treated as a fixed constant rather than a function of band width or sky
# model size. See estimate_di_calibrate_peak_gpu_bytes in
# calvin/hyperdrive.py.
HYPERDRIVE_GPU_RESIDENT_FIXED_BYTES = 2905 * 1024**2

# Fixed ceiling on concurrent GPU-bound hyperdrive workers, independent of
# whatever the GPU-memory cap alone would allow. Empirically measured on
# Calvin hardware: one hyperdrive process already saturates the shared
# GPU's compute, so additional concurrent workers add queueing overhead
# rather than real parallelism. Batch throughput was still improving at 6
# concurrent workers (the highest tested), but sustained CPU saturation
# grew from ~2 minutes at 4 workers to ~3 minutes at 6 -- capped here to
# keep some CPU headroom rather than chase the last bit of throughput. See
# _max_hyperdrive_workers in calvin/hyperdrive.py.
HYPERDRIVE_MAX_CONCURRENT_GPU_WORKERS = 6

# Per-component GPU byte costs for power-law sky-model components, double
# precision -- see the bindgen-generated size_of assertions for
# LmnRime/GaussianParams in mwa_hyperdrive: src/gpu/types_double.rs. Used
# by estimate_di_calibrate_peak_gpu_bytes.
HYPERDRIVE_GPU_LMN_BYTES = 24  # 3 x f64
HYPERDRIVE_GPU_GAUSSIAN_PARAMS_BYTES = 24  # 3 x f64

# hyperdrive DI-calibrate convergence "precision" (RESULTS HDU /
# CalibrationResult.max_precision: the largest change in any antenna's Jones
# solution on the final iteration; smaller == better converged). This equals
# hyperdrive's default min_threshold, so it serves two roles in
# HyperfitsSolutionGroup.weights: chanblocks worse than this are dropped, and
# it is the e-folding scale for the exp(-precision/scale) fit-weighting, so a
# chanblock right at the threshold keeps weight ~1/e instead of zero.
HYPERDRIVE_CONVERGENCE_PRECISION_MAX = 1e-4

# Reference-tile selection gates (calvin/hyperfits_solution_group.py
# select_refant). A tile below phase/gain fit quality, or whose phase-fit
# residual scatter exceeds the max, in EITHER polarisation, fails that gate
# -- see docs/REF_TILE_SELECTION.md.
REFTILE_PHASE_QUALITY_MIN = 0.8
# Max phase-fit residual std (radians, unweighted) to pass the phase-scatter
# gate. ~0.15 rad (~8.6 deg) is roughly 2-3x the scatter of a clean tile
# (~0.05-0.07 rad), so it passes good tiles while rejecting scattered fits.
REFTILE_PHASE_SIGMA_RESID_MAX = 0.15
REFTILE_GAIN_QUALITY_MIN = 0.8
REFTILE_DIPOLE_GAINS_EXPECTED = 32  # Total dipole gains per tile (16 X + 16 Y)
REFTILE_DIPOLE_GOOD_MIN = 32  # Min good dipoles (== 1.0) to pass the dipole gate
REFTILE_NAN_CHANNEL_FRACTION_MAX = 0.30  # Max NaN channel fraction to pass the channel gate
REFTILE_REPORT_TILES_TO_DISPLAY = 999  # Max number of tiles to display in the ref_tile selection report in stats.txt
