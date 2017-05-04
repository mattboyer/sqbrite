from setuptools.command.sdist import sdist as SetuptoolsSdist
from setuptools import setup, find_packages
import os
import shutil

import version
from src import PROJECT_NAME, PROJECT_DESCRIPTION, README_PATH


class SdistAndClean(SetuptoolsSdist):
    '''
    Runs the default setuptools sdist command and then cleans the egg info
    directory.
    '''

    def run(self):
        SetuptoolsSdist.run(self)

        # FIXME This works, but there *has* to be a cleaner way
        for distfile in self.filelist.files:
            if distfile.endswith('PKG-INFO'):
                egginfo_dir = os.path.dirname(distfile)
                shutil.rmtree(egginfo_dir)


def package_names():
    return [PROJECT_NAME] + \
        [PROJECT_NAME + '.' + package for package in find_packages('src')]

long_description = None
with open(README_PATH, 'r') as readme:
    long_description = readme.read()

setup(
    cmdclass={
        'sdist': SdistAndClean,
    },
    name=PROJECT_NAME,
    version=version.get_git_version(),
    url='https://github.com/mattboyer/sqbrite',
    description=PROJECT_DESCRIPTION,
    long_description=long_description or PROJECT_DESCRIPTION,
    author='Matt Boyer',
    author_email='mboyer@sdf.org',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database',
        'Topic :: System :: Recovery Tools',
    ],
    packages=package_names(),
    # Packaging data files in Python is a complete shitshow
    # We need this *AND* an "include" line in MANIFEST.IN
    include_package_data=True,
    package_dir={PROJECT_NAME: 'src'},
    install_requires=[
        'pyxdg',
        'pyyaml',
    ],
    entry_points={
        'console_scripts': [
            PROJECT_NAME+'='+PROJECT_NAME+'.sqlite_recover:main',
        ],
    },
)
