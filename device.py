import subprocess
import socket
import struct
import re
from typing import Optional

class RDMADeviceInfo:
    def __init__(self, port_num: int, device_index: int):
        self.port_num = port_num
        self.device_index = device_index

def is_ip_in_subnet(ip: str, subnet: str, mask: str) -> bool:
    ip_addr = struct.unpack('!I', socket.inet_aton(ip))[0]
    subnet_addr = struct.unpack('!I', socket.inet_aton(subnet))[0]
    mask_addr = struct.unpack('!I', socket.inet_aton(mask))[0]
    return (ip_addr & mask_addr) == (subnet_addr & mask_addr)

def execute_command(cmd: str) -> str:
    try:
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        return result.stdout.strip()
    except Exception as e:
        print(f"Failed to run command: {e}")
        return ""

def get_ip_address(iface: str) -> Optional[str]:
    cmd = f"ip -4 addr show {iface} | grep inet | awk '{{print $2}}'"
    output = execute_command(cmd)
    if output:
        return output.split('/')[0]
    return None

def find_device_index(subnet: str, mask: str) -> RDMADeviceInfo:
    dev_info = RDMADeviceInfo(0, -1)
    output = execute_command("ibdev2netdev")
    if not output:
        return dev_info

    dev_list_output = execute_command("ibv_devices")
    if not dev_list_output:
        print("Failed to get IB devices list.")
        return dev_info

    dev_list = []
    for line in dev_list_output.splitlines():
        if line.strip() and not line.strip().startswith('device') and not line.strip().startswith('------'):
            parts = line.split()
            if len(parts) >= 2:
                dev_list.append(parts[0].strip())
    num_devices = len(dev_list)
    if num_devices == 0:
        print("Failed to get IB devices list.")
        return dev_info

    lines = output.split('\n')
    pattern = re.compile(r"(\S+) port (\d+) ==> (\S+) \((\S+)\)")

    for line in lines:
        match = pattern.match(line)
        if match:
            mlx_name, port, iface, status = match.groups()
            port = int(port)
            if status == "Up":
                ip = get_ip_address(iface)
                if ip and is_ip_in_subnet(ip, subnet, mask):
                    if mlx_name in dev_list:
                        idx = dev_list.index(mlx_name)
                        dev_info.port_num = port
                        dev_info.device_index = idx
                        return dev_info
    return dev_info