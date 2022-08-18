# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# This file contains functions used by multiple commands
# -----------------------------------------------------------------------------
from os import environ
from os.path import join, expanduser
from configparser import ConfigParser
from qiita_client import QiitaClient


plugin_details = {'name': 'qp-mpileup-consensus',
                  'version': '2022.08',
                  'description': 'samtools mpileup pipeline'}


def client_connect(url):
    name = plugin_details['name']
    version = plugin_details['version']

    config = ConfigParser()
    conf_dir = environ.get(
        'QIITA_PLUGINS_DIR', join(expanduser('~'), '.qiita_plugins'))
    conf_fp = join(conf_dir, f'{name}_{version}.conf')

    with open(conf_fp, 'U') as conf_file:
        config.readfp(conf_file)
    qclient = QiitaClient(url, config.get('oauth2', 'CLIENT_ID'),
                          config.get('oauth2', 'CLIENT_SECRET'),
                          server_cert=config.get('oauth2', 'SERVER_CERT'))

    return qclient
