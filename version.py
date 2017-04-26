# -*- coding: utf-8 -*-
# Author: Douglas Creager <dcreager@dcreager.net>
# This file is placed into the public domain.

# Calculates the current version number.  If possible, this is the
# output of "git describe", modified to conform to the versioning
# scheme that setuptools uses.  If "git describe" returns an error
# (most likely because we're in an unpacked copy of a release tarball,
# rather than in a git working copy), then we fall back on reading the
# contents of the RELEASE-VERSION file.
#
# To use this script, simply import it your setup.py file, and use the
# results of get_git_version() as your package version:
#
# from version import *
#
# setup(
#     version=get_git_version(),
#     .
#     .
#     .
# )
#
# This will automatically update the RELEASE-VERSION file, if
# necessary.  Note that the RELEASE-VERSION file should *not* be
# checked into git; please add it to your top-level .gitignore file.
#
# You'll probably want to distribute the RELEASE-VERSION file in your
# sdist tarballs; to do this, just create a MANIFEST.in file that
# contains the following line:
#
#   include RELEASE-VERSION

from __future__ import print_function

import os
import subprocess

__all__ = ("get_git_version")


def call_git_describe(abbrev=4):
    runner = GitRunner()
    output = runner.run_git(['rev-parse', '--abbrev-ref', 'HEAD'])
    branch = output[0].strip()

    output = runner.run_git(['describe', '--long', '--abbrev=%d' % abbrev])
    tag = output[0].strip()
    release, commits_ahead, _ = tag.split('-')
    commits_ahead = int(commits_ahead)
    if commits_ahead:
        if 'master' == branch:
            return "{t}.post{c}".format(t=release, c=commits_ahead)
        else:
            return "{t}.dev{c}".format(t=release, c=commits_ahead)
    else:
        return release


def read_release_version():
    try:
        with open(get_release_version_path(), "r") as f:
            version = f.readlines()[0]
            return version.strip()
    except (IOError, OSError):
        return None


def get_release_version_path():
    top_level_dir = os.path.dirname(os.path.abspath(__file__))
    assert os.path.isdir(top_level_dir)
    rv_path = os.path.join(top_level_dir, 'RELEASE-VERSION')
    return rv_path


def write_release_version(version):
    f = open(get_release_version_path(), "w")
    f.write("%s\n" % version)
    f.close()


def get_git_version(abbrev=4):
    # Read in the version that's currently in RELEASE-VERSION.
    release_version = read_release_version()

    # First try to get the current version using "git describe".
    try:
        version = call_git_describe(abbrev)
    except GitError:
        # We're probably operating from a source dist
        version = None

    # If that doesn't work, fall back on the value that's in
    # RELEASE-VERSION.
    if version is None:
        version = release_version

    # If we still don't have anything, that's an error.
    if version is None:
        raise ValueError("Cannot find the version number!")

    # If the current version is different from what's in the
    # RELEASE-VERSION file, update the file to be current.
    if version != release_version:
        write_release_version(version)

    # Finally, return the current version.
    return version


class GitError(Exception):
    pass


class GitRunner(object):
    _toplevel_args = ['rev-parse', '--show-toplevel']
    _version_args = ['--version']
    _git_executable = 'git'
    _min_binary_ver = (1, 7, 2)

    def __init__(self):
        self._git_toplevel = None
        self._get_git_root()

    def _get_git_root(self):
        # We should probably go beyond just finding the root dir for the Git
        # repo and do some sanity-checking on git itself
        top_level_dir = self.run_git(GitRunner._toplevel_args)
        self._git_toplevel = top_level_dir[0]

    def run_git(self, args, git_env=None):
        '''
        Runs the git executable with the arguments given and returns a list of
        lines produced on its standard output.
        '''

        popen_kwargs = {
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE,
        }

        if git_env:
            popen_kwargs['env'] = git_env

        if self._git_toplevel:
            popen_kwargs['cwd'] = self._git_toplevel

        git_process = subprocess.Popen(
            [GitRunner._git_executable] + args,
            **popen_kwargs
        )

        try:
            out, err = git_process.communicate()
            git_process.wait()
        except Exception as e:
            raise GitError("Couldn't run 'git {args}':{newline}{ex}".format(
                args=' '.join(args),
                newline=os.linesep,
                ex=str(e)
            ))

        if (0 != git_process.returncode) or err:
            if err:
                err = err.decode('utf_8')
            raise GitError("'git {args}' failed with:{newline}{err}".format(
                args=' '.join(args),
                newline=os.linesep,
                err=err
            ))

        if not out:
            raise ValueError("No output")

        return out.decode('utf_8').splitlines()


if __name__ == "__main__":
    print(get_git_version())
