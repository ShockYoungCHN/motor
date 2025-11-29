#pragma once

#include <cstddef>

// Preferred CPU order for up to 32 worker/app threads. The list reflects
// NUMA-friendly cores the NIC shares a socket with.
static constexpr int kPreferredCpuList[] = {
        8, 9, 10, 11, 12, 13, 14, 15,
        0, 1, 2, 3, 4, 5, 6, 7,
        16, 17, 18, 19, 20, 21, 22, 23,
        24, 25, 26, 27, 28, 29, 30, 31,
};
static constexpr size_t kPreferredCpuCount =
    sizeof(kPreferredCpuList) / sizeof(kPreferredCpuList[0]);
static constexpr int kBgThreadFallbackCpu = 63;
static constexpr int kMainThreadFallbackCpu = 62;

static inline bool try_get_preferred_worker_cpu(int logical_index, int* cpu_id)
{
    if (logical_index >= 0 &&
        static_cast<size_t>(logical_index) < kPreferredCpuCount) {
        *cpu_id = kPreferredCpuList[logical_index];
        return true;
    }
    return false;
}

static inline int preferred_worker_cpu(int logical_index)
{
    int cpu_id;
    if (try_get_preferred_worker_cpu(logical_index, &cpu_id)) {
        return cpu_id;
    }

    // Fallback to the previous scheme so extra threads still get a core.
    return 2 * logical_index + 1;
}