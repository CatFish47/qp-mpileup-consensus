# -----------------------------------------------------------------------------
# Copyright (c) 2020, Qiita development team.
#
# Distributed under the terms of the BSD 3-clause License License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from qiita_client import QiitaPlugin, QiitaCommand
from .qp_mpileup_consensus import get_ref, mpileup_consensus
from .utils import plugin_details
from os.path import splitext


THREADS = 15


# Initialize the plugin
plugin = QiitaPlugin(**plugin_details)

# Define the command
ref = get_ref()
ref_without_extension = splitext(ref)[0]
# dbs_defaults = ', '.join([f'"{x}"' for x in dbs_without_extension])
req_params = {'input': ('artifact', ['BAM'])}
opt_params = {
    'reference': ['string', f'{ref_without_extension}']}

outputs = {'Consensus files': 'FASTA'}
default_params = {
    'default params': {
        'reference': "covid-ref"}}
# for db in dbs_without_extension:
#     name = f'auto-detect adapters and {db} + phix filtering'
#     default_params[name] = {'reference': db, 'threads': THREADS}

mpileup_consensus_cmd = QiitaCommand(
    'mpileup and consensus', "Samtools mpileup and ivar consensus",
    mpileup_consensus, req_params, opt_params, outputs, default_params)

plugin.register_command(mpileup_consensus_cmd)
