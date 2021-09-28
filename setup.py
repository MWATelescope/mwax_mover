from setuptools import setup, find_packages

setup(
   name='mwax_mover',
   version='0.3.26',
   description='A module to manage input and archiving for the MWAX correlator',
   author='Greg Sleap',
   author_email='',
   packages=find_packages(),
   entry_points={'console_scripts': [
        'mwax_mover = mwax_mover.mwax_mover:main',
        'mwax_subfile_distributor = mwax_mover.mwax_subfile_distributor:main',
        'mwacache_archiver = mwax_mover.mwacache_archive_processor:main']
    }
)
