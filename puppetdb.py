#!/usr/bin/env python
#
#   Copyright (c) 2015 iWeb Technologies Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#   Author: David Moreau Simard <dmsimard@iweb.com>
#
from __future__ import print_function
import argparse
import collections
import os
import sys
import time
import yaml
from uuid import getnode as get_mac
from pypuppetdb import connect

# Try to use the fastest json lib available
# Resort back to stdlib if necessary (slower)
try:
    import ujson as json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import json


mac = get_mac()

def load_config():
    """
    Looks for and loads yml configuration files
    """
    script_path = os.path.dirname(os.path.realpath(__file__))
    config_file_path = script_path + '/puppetdb.yml'

    if os.path.exists(config_file_path):
        try:
            with open(config_file_path) as fp:
                config = yaml.safe_load(fp)
        except yaml.YAMLError as err1:
            raise RuntimeError('ERROR: Could not parse YAML config file ' + str(err1))
    else:
        config = {
            "host": "localhost",
            "timeout": 10,
            "cache_duration": 15,
            "group_by": None,
            "port": 8080,
            "ssl_verify": False,
            "ssl_key": None,
            "ssl_cert": None,
            "group_by_tag": None
        }

    return config


class PuppetdbInventory(object):
    """
    A class that wraps around pypuppetdb to return ansible-compatible host
    lists and their hostvars (facts) based on data provided by PuppetDB.
    """

    def __init__(self, refresh):
        self.config = load_config()
        self.mac = ':'.join(("%012X" % mac)[i:i+2] for i in range(0, 12, 2))
        if not self.config:
            sys.exit('Error: Could not load any config files: {0}'
                     .format(', '.join(CONFIG_FILES)))

        puppetdb_config = {
            'host': self.config.get('host'),
            'port': self.config.get('port'),
            'timeout': self.config.get('timeout'),
            'ssl_verify': self.config.get('ssl_verify'),
            'ssl_key': self.config.get('ssl_key') or None,
            'ssl_cert': self.config.get('ssl_cert') or None
        }

        self.puppetdb = connect(**puppetdb_config)

        self.cache_file = os.path.expanduser('~') + '/ansible-inventory-puppetdb.cache'
        self.cache_duration = self.config.get('cache_duration')
        self.refresh = refresh

    def is_cache_stale(self):
        """
        Validates whether or not the cache file is stale
        """
        if os.path.isfile(self.cache_file):
            mod_time = os.path.getmtime(self.cache_file)
            current_time = time.time()
            if (mod_time + self.cache_duration) > current_time:
                return False

        return True

    def write_cache(self, groups):
        """
        Writes to the cache file
        """
        with open(self.cache_file, 'w') as cache_file:
            cache_file.write(groups)
        # change file permission so that only current user has access to this.
        os.chmod(self.cache_file, 0600)

    def get_host_list(self):
        """
        Updates the cache file, if necessary, and returns the inventory from it
        """
        if self.is_cache_stale() or self.refresh:
            groups = self.fetch_host_list()
            self.write_cache(groups)

        groups = json.load(open(self.cache_file, 'r'))
        return json.dumps(groups)

    def get_host_detail(self, host):
        """
        Returns data for a specific host only
        """
        facts = {
            host: self.fetch_host_facts(host)
        }

        return json.dumps(facts)

    def fetch_host_facts(self, host):
        """
        Fetch all fact and their values for a given host
        """
        node = self.puppetdb.node(host)
        facts = dict((fact.name, fact.value) for fact in node.facts())

        facts['ansible_host'] = node.fact('ipaddress').value

        return facts

    def fetch_tag_results(self, tag_lookup):
        """
        Fetch all hosts based upon resource type tag
        """
        hosts = []

        for resource_type, tag in tag_lookup.iteritems():
            resources = self.puppetdb.resources(
                type_="{0}".format(resource_type),
                query='["=", "tag", "{0}"]'.format(tag))
            for host in resources:
                hosts.append(host.node)
        return hosts

    def fetch_host_list(self):
        """
        Returns data for all hosts found in PuppetDB
        """
        groups = collections.defaultdict(dict)
        hostvars = collections.defaultdict(dict)

        groups['all']['hosts'] = list()
        hostvars_local = {}

        group_by = self.config.get('group_by')
        group_by_tag = self.config.get('group_by_tag')

        for node in self.puppetdb.nodes():
            server = str(node)

            if group_by is not None:
                try:
                    fact_value = node.fact(group_by).value
                    if fact_value not in groups:
                        groups[fact_value]['hosts'] = list()
                    groups[fact_value]['hosts'].append(server)
                except StopIteration:
                    # This fact does not exist on the server
                    if 'unknown' not in groups:
                        groups['unknown']['hosts'] = list()
                    groups['unknown']['hosts'].append(server)

            if group_by_tag:
                for entry in group_by_tag:
                    for resource_type, tag in entry.iteritems():
                        tag_lookup = {resource_type: tag}
                        tagged_hosts = self.fetch_tag_results(tag_lookup)
                        group_key = tag
                        if server in tagged_hosts:
                            if group_key not in groups:
                                groups[group_key]['hosts'] = list()
                            groups[group_key]['hosts'].append(server)

            groups['all']['hosts'].append(server)
            host_fact = self.fetch_host_facts(server)
            hostvars[server] = host_fact
            if host_fact['macaddress'].upper() == self.mac:
                hostvars_local = host_fact
            groups['_meta'] = {'hostvars': hostvars}

        groups['all']['hosts'].append('local')
        groups['_meta']['hostvars']['local'] = hostvars_local
        groups['_meta']['hostvars']['local']['ansible_connection'] = 'local'
        groups['_meta']['hostvars']['local']['ansible_host'] = '127.0.0.1'
        return json.dumps(groups)


def parse_args():
    """
    Parses script arguments.
    """
    parser = argparse.ArgumentParser(description='PuppetDB Inventory Module')
    parser.add_argument('--refresh', action='store_true', default=False,
                        help='Refreshes cached information')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--list', action='store_true',
                       help='List servers known by PuppetDB')
    group.add_argument('--host',
                       help='List details about specified host')
    return parser.parse_args()


def main():
    """
    Instanciate the inventory and return a single host or a list of hosts
    """
    args = parse_args()

    inventory = PuppetdbInventory(args.refresh)
    if args.list:
        print(inventory.get_host_list())

    if args.host:
        print(inventory.get_host_detail(args.host))


if __name__ == '__main__':
    main()
