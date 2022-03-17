# MIT License
#
# Copyright (c) 2017 Matt Boyer
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging
import os
import xdg.BaseDirectory


PROJECT_NAME = 'sqbrite'
PROJECT_DESCRIPTION = 'SQBrite is a data recovery tool for SQLite databases'

USER_YAML_PATH = os.path.join(
    xdg.BaseDirectory.save_data_path(PROJECT_NAME),
    PROJECT_NAME + '.yaml'
)

BUILTIN_YAML = 'magic/sqbrite.yaml'

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')
_LOGGER = logging.getLogger('SQLite recovery')
_LOGGER.setLevel(logging.INFO)
