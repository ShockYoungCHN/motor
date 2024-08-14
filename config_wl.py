import os, json, sys
import subprocess, socket

# modify the workload in flags.h and mn_config.json
def configure_workload(flag_path, config_path, workload):
    with open(flag_path, 'r') as file:
        lines = file.readlines()

    workloads = {
        'tpcc': 'WORKLOAD_TPCC',
        'tatp': 'WORKLOAD_TATP',
        'smallbank': 'WORKLOAD_SmallBank',
        'micro': 'WORKLOAD_MICRO'
    }
    if workload not in workloads:
        raise ValueError(f"Invalid workload type: {workload}. Choose from {list(workloads.keys())}.")

    with open(flag_path, 'w') as file:
        for line in lines:
            if line.startswith('#define WORKLOAD') and any(wl in line for wl in workloads.values()):
                if workloads[workload] in line:
                    file.write(f"#define {workloads[workload]} 1\n")
                else:
                    file.write(f"#define {line.split()[1]} 0\n")
            else:
                file.write(line)


def get_local_lan_ip():
    result = subprocess.run(['ifconfig'], stdout=subprocess.PIPE)
    output = result.stdout.decode()

    for line in output.splitlines():
        line = line.strip()
        if line.startswith('inet ') and '192.168' in line:
            ip = line.split()[1]
            return ip

    return None


def config_memory_node(config_path):
    local_ip = get_local_lan_ip()
    assert local_ip is not None, "Local IP not found."

    ip_list = ["192.168.1.2", "192.168.1.3", "192.168.1.4"]

    memory_node_ips = [ip for ip in ip_list if ip != local_ip]

    memory_node_ids = [int(ip.split('.')[-1]) - 2 for ip in memory_node_ips]

    with open(config_path, 'r') as file:
        config_data = json.load(file)

    config_data["other_memory_nodes"]["memory_node_ips"] = memory_node_ips
    config_data["other_memory_nodes"]["memory_node_ids"] = memory_node_ids
    config_data["local_memory_node"]["machine_id"] = int(local_ip.split('.')[-1]) - 2


    workloads_to_upper_case = {
        'tpcc': 'TPCC',
        'tatp': 'TATP',
        'smallbank': 'SmallBank',
        'micro': 'MICRO'
    }
    if "local_memory_node" in config_data and "workload" in config_data["local_memory_node"]:
        config_data["local_memory_node"]["workload"] = workloads_to_upper_case[workload]

    with open(config_path, 'w') as file:
        json.dump(config_data, file, indent=4)

    print("Configuration updated successfully.")


if __name__ == "__main__":
    flag_path = os.path.join(os.path.dirname(__file__), "txn", "flags.h")
    mn_config_path = os.path.join(os.path.dirname(__file__), "config", "mn_config.json")
    workload = sys.argv[1]
    is_memory = sys.argv[2]
    configure_workload(flag_path, mn_config_path, workload)
    if is_memory == "true":
        config_memory_node(mn_config_path)

    result = subprocess.run(["./build.sh"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_str = result.stdout.decode('utf-8')
    stderr_str = result.stderr.decode('utf-8')
    if "-------------------- build finish ----------------------" in stdout_str:
        print("Workload configured successfully.")
    else:
        print("Error configuring workload.")
        print(stderr_str)