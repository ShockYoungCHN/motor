import os
# this file reads all the IB counters from /sys/class/infiniband/<dev_name>/ports/1/hw_counters/ and
# /sys/class/infiniband/<dev_name>/ports/1/counters/

def get_hw_counters(dev_name):
    hw_counters_path = f"/sys/class/infiniband/{dev_name}/ports/1/hw_counters"
    return read_counters_from_directory(hw_counters_path)


def get_counters(dev_name):
    counters_path = f"/sys/class/infiniband/{dev_name}/ports/1/counters"
    return read_counters_from_directory(counters_path)


def read_counters_from_directory(directory_path):
    counters = {}
    if not os.path.exists(directory_path):
        print(f"Path does not exist: {directory_path}")
        return counters

    for file_name in os.listdir(directory_path):
        file_path = os.path.join(directory_path, file_name)
        try:
            with open(file_path, "r") as f:
                value = f.read().strip()
                counters[file_name] = value
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    return counters

class IBCounterMonitor:
    def __init__(self, dev_name):
        self.dev_name = dev_name

    def start(self):
        self.hw_counters_start = get_hw_counters(self.dev_name)
        self.counters_start = get_counters(self.dev_name)

    def end(self):
        self.hw_counters_end = get_hw_counters(self.dev_name)
        self.counters_end = get_counters(self.dev_name)

    def get_counters_diff(self):
        hw_counters_diff = {}
        counters_diff = {}
        for key in self.hw_counters_start:
            hw_counters_diff[key] = int(self.hw_counters_end[key]) - int(self.hw_counters_start[key])
        for key in self.counters_start:
            counters_diff[key] = int(self.counters_end[key]) - int(self.counters_start[key])
        return hw_counters_diff, counters_diff


