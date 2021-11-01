#!/usr/bin/env python3

#
# @source https://apple.stackexchange.com/a/345540
#

import re
import subprocess
import sys

def service_name_for_interface(interface):
    return 'wg-updown-' + interface

v4pat = re.compile(r'^\s*inet\s+(\S+)\s+-->\s+(\S+)\s+netmask\s+\S+')
v6pat = re.compile(r'^\s*inet6\s+(\S+?)(?:%\S+)?\s+prefixlen\s+(\S+)')
def get_tunnel_info(interface):
    ipv4s = dict(Addresses=[], DestAddresses=[])
    ipv6s = dict(Addresses=[], DestAddresses=[], Flags=[], PrefixLength=[])
    ifconfig = subprocess.run(["ifconfig", interface], capture_output=True,
                              check=True, text=True)
    for line in ifconfig.stdout.splitlines():
        v6match = v6pat.match(line)
        if v6match:
            ipv6s['Addresses'].append(v6match[1])
            # This is cribbed from Viscosity and probably wrong.
            if v6match[1].startswith('fe80'):
                ipv6s['DestAddresses'].append('::ffff:ffff:ffff:ffff:0:0')
            else:
                ipv6s['DestAddresses'].append('::')
            ipv6s['Flags'].append('0')
            ipv6s['PrefixLength'].append(v6match[2])
            continue
        v4match = v4pat.match(line)
        if v4match:
            ipv4s['Addresses'].append(v4match[1])
            ipv4s['DestAddresses'].append(v4match[2])
            continue
    return (ipv4s, ipv6s)

def run_scutil(commands):
    print(commands)
    subprocess.run(['scutil'], input=commands, check=True, text=True)

def up(interface):
    service_name = service_name_for_interface(interface)
    (ipv4s, ipv6s) = get_tunnel_info(interface)
    run_scutil('\n'.join([
        f"d.init",
        f"d.add Addresses * {' '.join(ipv4s['Addresses'])}",
        f"d.add DestAddresses * {' '.join(ipv4s['DestAddresses'])}",
        f"d.add InterfaceName {interface}",
        f"set State:/Network/Service/{service_name}/IPv4",
        f"set Setup:/Network/Service/{service_name}/IPv4",
        f"d.init",
        f"d.add Addresses * {' '.join(ipv6s['Addresses'])}",
        f"d.add DestAddresses * {' '.join(ipv6s['DestAddresses'])}",
        f"d.add Flags * {' '.join(ipv6s['Flags'])}",
        f"d.add InterfaceName {interface}",
        f"d.add PrefixLength * {' '.join(ipv6s['PrefixLength'])}",
        f"set State:/Network/Service/{service_name}/IPv6",
        f"set Setup:/Network/Service/{service_name}/IPv6",
    ]))

def down(interface):
    service_name = service_name_for_interface(interface)
    run_scutil('\n'.join([
        f"remove State:/Network/Service/{service_name}/IPv4",
        f"remove Setup:/Network/Service/{service_name}/IPv4",
        f"remove State:/Network/Service/{service_name}/IPv6",
        f"remove Setup:/Network/Service/{service_name}/IPv6",
    ]))

def main():
    operation = sys.argv[1]
    interface = sys.argv[2]
    if operation == 'up':
        up(interface)
    elif operation == 'down':
        down(interface)
    else:
        raise NotImplementedError()

if __name__ == "__main__":
    main()
