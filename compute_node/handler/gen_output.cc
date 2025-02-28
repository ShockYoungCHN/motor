// Author: Ming Zhang
// Copyright (c) 2023

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <mutex>
#include <numeric>
#include <thread>
#include <iomanip>

#include "handler/handler.h"
#include "handler/worker.h"
#include "process/oplog.h"
#include "process/stat.h"
#include "util/json_config.h"

///////////// For control and statistics ///////////////
std::atomic<uint64_t> tx_id_generator;
std::atomic<uint64_t> connected_t_num;
std::atomic<uint64_t> connected_recovery_t_num;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> avg_lat;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
std::vector<uint64_t> total_try_times;
std::vector<uint64_t> total_commit_times;
std::vector<double> delta_usage;

#if DATA_ACCOUNTING
std::vector<uint64_t> read_bytes;
std::vector<uint64_t> write_bytes;
std::vector<uint64_t> read_cnts;
std::vector<uint64_t> write_cnts;
std::vector<uint64_t> CAS_cnts;
Latency rw_hash_latency[7];
Latency rw_val_latency[7];
Latency ro_hash_latency[7];
Latency ro_val_latency[7];
Latency poll_cq_latency[7];
#endif

#if TX_PHASE_LATENCY
// txn type-wise latency
extern Latency exe_succ_latencies[7];
extern Latency exe_fail_latencies[7];
extern Latency validate_succ_latencies[7];
extern Latency validate_fail_latencies[7];
extern Latency commit_latencies[7];
extern Latency abort_latencies[7];
#endif

// Get the frequency of accessing old versions
uint64_t access_old_version_cnt[MAX_TNUM_PER_CN];
uint64_t access_new_version_cnt[MAX_TNUM_PER_CN];

EventCount event_counter;
KeyCount key_counter;

// For crash recovery test
std::atomic<bool> to_crash[MAX_TNUM_PER_CN];
std::atomic<bool> report_crash[MAX_TNUM_PER_CN];
uint64_t try_times[MAX_TNUM_PER_CN];

std::atomic<bool> primary_fail;
std::atomic<bool> cannot_lock_new_primary;

std::atomic<bool> one_backup_fail;
std::atomic<bool> during_backup_recovery;

// For probing
std::atomic<int> probe_times;
std::atomic<bool> probe[MAX_TNUM_PER_CN];
std::vector<std::vector<TpProbe>> tp_probe_vec;
std::atomic<bool> is_running;

/////////////////////////////////////////////////////////

void TimeStop(t_id_t thread_num_per_machine, int tp_probe_interval_us) {
  while (is_running) {
    usleep(tp_probe_interval_us);
    for (int i = 0; i < thread_num_per_machine; i++) {
      probe[i] = true;
    }
    probe_times++;
  }
}

void Handler::GenThreads(std::string bench_name) {
  std::string config_filepath = "../../../config/cn_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  t_id_t thread_num_per_machine = (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();
  int crash_tnum = 0;

#if HAVE_COORD_CRASH
  crash_tnum = (int)client_conf.get("crash_tnum").get_int64();
#endif
  assert(machine_id >= 0 && machine_id < machine_num && thread_num_per_machine > 2 * crash_tnum);

  AddrCache* addr_caches = new AddrCache[thread_num_per_machine - crash_tnum];

  for (int i = 0; i < MAX_TNUM_PER_CN; i++) {
    access_old_version_cnt[i] = 0;
    access_new_version_cnt[i] = 0;
  }

  /*** Coordinator crash model
   * total: 40 threads
   * crash: 10 threads
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * |       20 good          |   10 will-crash    |   10 prepare   |
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   */

#if HAVE_PRIMARY_CRASH
  primary_fail = false;
  cannot_lock_new_primary = false;
#endif

#if HAVE_BACKUP_CRASH
  one_backup_fail = false;
  during_backup_recovery = false;
#endif

#if HAVE_COORD_CRASH
  // Prepare crash info

  for (int i = 0; i < thread_num_per_machine; i++) {
    to_crash[i] = false;
    report_crash[i] = false;
  }
  memset((char*)try_times, 0, sizeof(try_times));
#endif

#if PROBE_TP
  probe_times = 0;
  for (int i = 0; i < thread_num_per_machine; i++) {
    probe[i] = false;
  }
  tp_probe_vec.resize(thread_num_per_machine);
#endif

  /* Start working */
  tx_id_generator = 1;  // Initial transaction id == 1
  connected_t_num = 0;  // Sync all threads' RDMA QP connections
  connected_recovery_t_num = 0;

  auto thread_arr = new std::thread[thread_num_per_machine];

  auto* global_meta_man = new MetaManager();
  RDMA_LOG(INFO) << "Alloc local memory: " << (size_t)(thread_num_per_machine * PER_THREAD_ALLOC_SIZE) / (1024 * 1024) << " MB. Waiting...";
  auto* global_rdma_region = new LocalRegionAllocator(global_meta_man, thread_num_per_machine);

  auto* global_delta_region = new RemoteDeltaRegionAllocator(global_meta_man, global_meta_man->remote_nodes);

  auto* global_locked_key_table = new LockedKeyTable[thread_num_per_machine * coro_num];

  auto* param_arr = new struct thread_params[thread_num_per_machine];

  TATP* tatp_client = nullptr;
  SmallBank* smallbank_client = nullptr;
  TPCC* tpcc_client = nullptr;

  if (bench_name == "tatp") {
    tatp_client = new TATP();
    total_try_times.resize(TATP_TX_TYPES, 0);
    total_commit_times.resize(TATP_TX_TYPES, 0);
  } else if (bench_name == "smallbank") {
    smallbank_client = new SmallBank();
    total_try_times.resize(SmallBank_TX_TYPES, 0);
    total_commit_times.resize(SmallBank_TX_TYPES, 0);
  } else if (bench_name == "tpcc") {
    tpcc_client = new TPCC();
    total_try_times.resize(TPCC_TX_TYPES, 0);
    total_commit_times.resize(TPCC_TX_TYPES, 0);
  } else if (bench_name == "micro") {
    total_try_times.resize(MICRO_TX_TYPES, 0);
    total_commit_times.resize(MICRO_TX_TYPES, 0);
  }

  RDMA_LOG(INFO) << "Running on isolation level: " << global_meta_man->iso_level;
  RDMA_LOG(INFO) << "Executing...";

  for (t_id_t i = 0; i < thread_num_per_machine - crash_tnum; i++) {
    param_arr[i].thread_local_id = i;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].addr_cache = &(addr_caches[i]);
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].global_delta_region = global_delta_region;
    param_arr[i].global_locked_key_table = global_locked_key_table;
    param_arr[i].running_tnum = thread_num_per_machine - crash_tnum;
    thread_arr[i] = std::thread(run_thread,
                                &param_arr[i],
                                tatp_client,
                                smallbank_client,
                                tpcc_client,
                                &(tp_probe_vec[i]));

    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(2*i+1, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }

#if PROBE_TP
  is_running = true;
  int tp_probe_inter_us = (int)client_conf.get("tp_probe_interval_ms").get_int64() * 1000;
  std::thread time_stop = std::thread(TimeStop, thread_num_per_machine, tp_probe_inter_us);
#endif

#if HAVE_PRIMARY_CRASH

  int crash_time_ms = (int)client_conf.get("crash_time_ms").get_int64();
  std::cerr << "sleeping " << (double)crash_time_ms / 1000.0 << " seconds..." << std::endl;
  usleep(crash_time_ms * 1000);

  std::cerr << "primary crashes!\n";

  primary_fail = true;
#endif

#if HAVE_BACKUP_CRASH
  int crash_time_ms = (int)client_conf.get("crash_time_ms").get_int64();
  std::cerr << "sleeping " << (double)crash_time_ms / 1000.0 << " seconds..." << std::endl;
  usleep(crash_time_ms * 1000);

  std::cerr << "backup crashes!\n";
  one_backup_fail = true;
#endif

#if HAVE_COORD_CRASH
  int crash_time_ms = (int)client_conf.get("crash_time_ms").get_int64();
  std::cerr << "sleeping " << (double)crash_time_ms / 1000.0 << " seconds..." << std::endl;
  usleep(crash_time_ms * 1000);

  // Make crash
  for (int k = thread_num_per_machine - crash_tnum - crash_tnum; k < thread_num_per_machine - crash_tnum; k++) {
    std::cerr << "Thread " << k << " should crash" << std::endl;
    to_crash[k] = true;
  }

  {
    // Print time
    time_t tt;
    struct timeval tv_;
    struct tm* timeinfo;
    long tv_ms = 0, tv_us = 0;
    char output[20];
    time(&tt);
    timeinfo = localtime(&tt);
    gettimeofday(&tv_, NULL);
    strftime(output, 20, "%Y-%m-%d %H:%M:%S", timeinfo);
    tv_ms = tv_.tv_usec / 1000;
    tv_us = tv_.tv_usec % 1000;
    printf("crash at :%s %ld:%ld\r\n", output, tv_ms, tv_us);
  }

  for (int crasher = thread_num_per_machine - crash_tnum - crash_tnum; crasher < thread_num_per_machine - crash_tnum; crasher++) {
    while (!report_crash[crasher])
      ;

    int i = crasher + crash_tnum;  // i is the recovery thread's id

    param_arr[i].thread_local_id = i;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].addr_cache = &(addr_caches[crasher]);
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].global_delta_region = global_delta_region;
    param_arr[i].global_locked_key_table = global_locked_key_table;
    param_arr[i].running_tnum = crash_tnum;
    thread_arr[i] = std::thread(recovery,
                                &param_arr[i],
                                tatp_client,
                                smallbank_client,
                                tpcc_client,
                                try_times[crasher],
                                &(tp_probe_vec[i]),
                                crasher);

    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }
#endif

  uint64_t total_addr_size = 0;
  uint64_t total_offset_size = 0;
  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    if (thread_arr[i].joinable()) {
      thread_arr[i].join();
      // RDMA_LOG(INFO) << "Thread " << i << " joins";
    }
    total_addr_size+=param_arr[i].addr_cache->TotalAddrSize();
    total_offset_size+=param_arr[i].addr_cache->TotalAddrSize(true);
  }
  std::cout << "addr size: " << total_addr_size <<", offset size: " << total_offset_size << "bytes,"
  << " #thread: " << thread_num_per_machine << std::endl;

#if PROBE_TP
  is_running = false;
  if (time_stop.joinable()) {
    time_stop.join();
    // RDMA_LOG(INFO) << "timer thread joins";
  }
#endif

  RDMA_LOG(INFO) << "DONE";

  delete[] addr_caches;
  delete[] global_locked_key_table;
  delete[] param_arr;
  delete global_rdma_region;
  delete global_meta_man;
  if (tatp_client) delete tatp_client;
  if (smallbank_client) delete smallbank_client;
  if (tpcc_client) delete tpcc_client;
}

void Handler::OutputResult(std::string bench_name, std::string system_name) {
#if WORKLOAD_SmallBank
  std::vector<std::string> tx_names = std::vector<std::string>(SmallBank_TX_NAME, SmallBank_TX_NAME + SmallBank_TX_TYPES);
#elif  WORKLOAD_TATP
  std::vector<std::string> tx_names = std::vector<std::string>(TATP_TX_NAME, TATP_TX_NAME + TATP_TX_TYPES);
#elif WORKLOAD_TPCC
  std::vector<std::string> tx_names = std::vector<std::string>(TPCC_TX_NAME, TPCC_TX_NAME + TPCC_TX_TYPES);
#endif

  RDMA_LOG(INFO) << "Generate results...";
  std::string results_cmd = "mkdir -p ../../../bench_results/" + bench_name;
  system(results_cmd.c_str());
  std::ofstream of, of_detail, of_delta_usage;
  std::string res_file = "../../../bench_results/" + bench_name + "/result.txt";
  std::string delta_usage_file = "../../../bench_results/" + bench_name + "/delta_usage.txt";

  of.open(res_file.c_str(), std::ios::app);
  // of_delta_usage.open(delta_usage_file.c_str(), std::ios::app);

  double total_attemp_tp = 0;
  double total_tp = 0;
  double total_median = 0;
  double total_tail = 0;

  for (int i = 0; i < tid_vec.size(); i++) {
    total_attemp_tp += attemp_tp_vec[i];
    total_tp += tp_vec[i];
    total_median += medianlat_vec[i];
    total_tail += taillat_vec[i];
  }

  size_t thread_num = tid_vec.size();

  double avg_median = total_median / thread_num;
  double avg_tail = total_tail / thread_num;

  std::sort(medianlat_vec.begin(), medianlat_vec.end());
  std::sort(taillat_vec.begin(), taillat_vec.end());

  of << system_name << " " << total_attemp_tp / 1000 << " " << total_tp / 1000 << " " << avg_median << " " << avg_tail << std::endl;
  of.close();

  std::ofstream of_abort_rate;
  std::string abort_rate_file = "../../../bench_results/" + bench_name + "/abort_rate.txt";
  of_abort_rate.open(abort_rate_file.c_str(), std::ios::app);
  of_abort_rate << system_name << " tx_type try_num commit_num abort_rate" << std::endl;
  if (bench_name == "tatp") {
    for (int i = 0; i < TATP_TX_TYPES; i++) {
      of_abort_rate << TATP_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;

      // Output the specific txn's abort rate
      std::string onetxn_abort_rate_file = "../../../bench_results/" + bench_name + "/" + TATP_TX_NAME[i] + "_abort_rate.txt";
      std::ofstream of_onetxn_abort_rate;
      of_onetxn_abort_rate.open(onetxn_abort_rate_file.c_str(), std::ios::app);
      of_onetxn_abort_rate << system_name << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }

  } else if (bench_name == "smallbank") {
    for (int i = 0; i < SmallBank_TX_TYPES; i++) {
      of_abort_rate << SmallBank_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;

      // Output the specific txn's abort rate
      std::string onetxn_abort_rate_file = "../../../bench_results/" + bench_name + "/" + SmallBank_TX_NAME[i] + "_abort_rate.txt";
      std::ofstream of_onetxn_abort_rate;
      of_onetxn_abort_rate.open(onetxn_abort_rate_file.c_str(), std::ios::app);
      of_onetxn_abort_rate << system_name << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }
  } else if (bench_name == "tpcc") {
    for (int i = 0; i < TPCC_TX_TYPES; i++) {
      of_abort_rate << TPCC_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;

      // Output the specific txn's abort rate
      std::string onetxn_abort_rate_file = "../../../bench_results/" + bench_name + "/" + TPCC_TX_NAME[i] + "_abort_rate.txt";
      std::ofstream of_onetxn_abort_rate;
      of_onetxn_abort_rate.open(onetxn_abort_rate_file.c_str(), std::ios::app);
      of_onetxn_abort_rate << system_name << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }
  } else if (bench_name == "micro") {
    for (int i = 0; i < MICRO_TX_TYPES; i++) {
      of_abort_rate << MICRO_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;

      // Output the specific txn's abort rate
      std::string onetxn_abort_rate_file = "../../../bench_results/" + bench_name + "/" + MICRO_TX_NAME[i] + "_abort_rate.txt";
      std::ofstream of_onetxn_abort_rate;
      of_onetxn_abort_rate.open(onetxn_abort_rate_file.c_str(), std::ios::app);
      of_onetxn_abort_rate << system_name << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }
  }

  of_abort_rate << std::endl;
  of_abort_rate.close();

  // todo: the avg lat of committed txn is not recorded to res_file
  auto avg = std::accumulate(avg_lat.begin(), avg_lat.end(), 0.0) / avg_lat.size();
  std::cout << system_name << " " << total_attemp_tp / 1000 << " " << total_tp / 1000 << " " << avg_median << " " << avg_tail << " " << avg << std::endl;

  double total_delta_usage_MB = 0;
  for (int i = 0; i < delta_usage.size(); i++) {
    total_delta_usage_MB += delta_usage[i];
  }

  // std::cout << "TOTAL delta: " << total_delta_usage_MB << " MB"
  //           << ". AVG delta/thread: " << (double)total_delta_usage_MB / thread_num << " MB" << std::endl;

  // of_delta_usage << system_name << " vnum: " << MAX_VCELL_NUM << " total_delta_usage_MB: " << total_delta_usage_MB << std::endl;
  // of_delta_usage.close();

#if OUTPUT_EVENT_STAT

  std::ofstream of_event_count("../../../event_count.yml", std::ofstream::out);

  of_event_count << "Abort Rate for all txns" << std::endl;
  of_event_count << system_name << " tx_type try_num commit_num abort_rate" << std::endl;

  std::cout << std::endl;
  std::cout << "abort rate:" << std::endl;
  if (bench_name == "tatp") {
    for (int i = 0; i < TATP_TX_TYPES; i++) {
      of_event_count << TATP_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
      std::cout << TATP_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }
  } else if (bench_name == "smallbank") {
    for (int i = 0; i < SmallBank_TX_TYPES; i++) {
      of_event_count << SmallBank_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
      std::cout << SmallBank_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }
  } else if (bench_name == "tpcc") {
    for (int i = 0; i < TPCC_TX_TYPES; i++) {
      of_event_count << TPCC_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
      std::cout << TPCC_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }
  } else if (bench_name == "micro") {
    for (int i = 0; i < MICRO_TX_TYPES; i++) {
      of_event_count << MICRO_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
      std::cout << MICRO_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }
  }

  event_counter.Output(of_event_count);

  of_event_count.close();
#endif

#if OUTPUT_KEY_STAT
  key_counter.Output();
#endif

#if PROBE_TP
  std::map<int, double> tp_time_figure;
  std::map<int, double> attemp_tp_time_figure;

  std::ofstream of_probe_tp;
  time_t rawtime;
  struct tm* ptminfo;
  time(&rawtime);
  ptminfo = localtime(&rawtime);
  std::string s;
  if (ptminfo->tm_mon + 1 < 10) {
    s = std::to_string(ptminfo->tm_year + 1900) + "-0" + std::to_string(ptminfo->tm_mon + 1) + "-" + std::to_string(ptminfo->tm_mday) + "@" + std::to_string(ptminfo->tm_hour) + ":" + std::to_string(ptminfo->tm_min) + ":" + std::to_string(ptminfo->tm_sec);
  } else {
    s = std::to_string(ptminfo->tm_year + 1900) + "-" + std::to_string(ptminfo->tm_mon + 1) + "-" + std::to_string(ptminfo->tm_mday) + "@" + std::to_string(ptminfo->tm_hour) + ":" + std::to_string(ptminfo->tm_min) + ":" + std::to_string(ptminfo->tm_sec);
  }

  std::string probe_file = "../../../bench_results/crash_tests/" + bench_name + "/tp_probe@" + system_name + "@" + s + ".txt";
  of_probe_tp.open(probe_file.c_str(), std::ios::out);
  for (int i = 0; i < tp_probe_vec.size(); i++) {
    for (int j = 0; j < tp_probe_vec[i].size(); j++) {
      // sum-up all threads' tp
      if (tp_time_figure.find(tp_probe_vec[i][j].ctr) == tp_time_figure.end()) {
        tp_time_figure[tp_probe_vec[i][j].ctr] = tp_probe_vec[i][j].tp;
      } else {
        tp_time_figure[tp_probe_vec[i][j].ctr] += tp_probe_vec[i][j].tp;
      }

      if (attemp_tp_time_figure.find(tp_probe_vec[i][j].ctr) == attemp_tp_time_figure.end()) {
        attemp_tp_time_figure[tp_probe_vec[i][j].ctr] = tp_probe_vec[i][j].attemp_tp;
      } else {
        attemp_tp_time_figure[tp_probe_vec[i][j].ctr] += tp_probe_vec[i][j].attemp_tp;
      }
    }
  }

  std::string config_filepath = "../../../config/cn_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  int tp_probe_interval_ms = (int)client_conf.get("tp_probe_interval_ms").get_int64();

  int start_time = tp_time_figure.begin()->first;
  auto iter = tp_time_figure.end();
  iter--;
  int end_time = iter->first;

  for (int i = start_time; i <= end_time; i++) {
    auto iter = tp_time_figure.find(i);
    if (iter != tp_time_figure.end()) {
      of_probe_tp << iter->first * tp_probe_interval_ms << " " << iter->second / 1000.0 << std::endl;
    } else {
      of_probe_tp << i << " " << 0 << std::endl;
    }
  }

  of_probe_tp.close();
#endif

  // below 2 flags are added by Yuanzhuo, they might not be compatible with the PROBE_TP, OUTPUT_KEY_STAT, OUTPUT_EVENT_STAT
  std::ofstream of_data_accounting;
  std::string data_accounting_file = "../../../data_accouting.txt";
  of_data_accounting.open(data_accounting_file.c_str(), std::ios::app);

  auto getAverage = [](const std::vector<uint64_t>& vec) -> uint64_t {
    if (vec.empty())
      throw std::invalid_argument("Data vector is empty");
    uint64_t sum = std::accumulate(vec.begin(), vec.end(), 0UL);
    return sum / vec.size();
  };
  auto getArrAverage = [](const std::vector<std::array<uint64_t, 3>>& data, int index) -> uint64_t {
    if (data.empty()) {
      throw std::invalid_argument("Data vector is empty");
    }
    uint64_t sum = 0;
    for (const auto& arr : data) {
      sum += arr[index];
    }
    return sum / data.size();
  };
#if DATA_ACCOUNTING
// ignore read_throughput write_throughput for now
  of_data_accounting << system_name << std::endl;
  of_data_accounting << "read_bytes write_bytes read_cnts write_cnts CAS_cnts" << std::endl;
  of_data_accounting << getAverage(read_bytes) << " " << getAverage(write_bytes) << " "
    << getAverage(read_cnts) << " " << getAverage(write_cnts) << " " << getAverage(CAS_cnts) << std::endl;
#endif

#if TX_PHASE_LATENCY == 1 && DATA_ACCOUNTING == 0
  std::cout << "tx_names.size(): " << tx_names.size() << std::endl;
  for (int i=0; i<tx_names.size(); i++)
  {
    of_data_accounting << tx_names[i] << " exec_succ_latency exec_fail_latency validate_succ_latency validate_fail_latency commit_latency abort_latency" << std::endl;
    of_data_accounting << std::fixed << std::setprecision(1);
    // p50
    double exe_succ_lat = exe_succ_latencies[i].perc(0.5)/10.0;
    double exe_fail_lat = exe_fail_latencies[i].perc(0.5)/10.0;
    double val_succ_lat = validate_succ_latencies[i].perc(0.5)/10.0;
    double val_fail_lat = validate_fail_latencies[i].perc(0.5)/10.0;
    double com_lat = commit_latencies[i].perc(0.5)/10.0;
    double abo_lat = abort_latencies[i].perc(0.5)/10.0;
    of_data_accounting << "p50" << " " << exe_succ_lat << " " << exe_fail_lat << " " << val_succ_lat << " " << val_fail_lat << " " << com_lat << " " << abo_lat << std::endl;

    // p99
    exe_succ_lat = exe_succ_latencies[i].perc(0.99)/10.0;
    exe_fail_lat = exe_fail_latencies[i].perc(0.99)/10.0;
    val_succ_lat = validate_succ_latencies[i].perc(0.99)/10.0;
    val_fail_lat = validate_fail_latencies[i].perc(0.99)/10.0;
    com_lat = commit_latencies[i].perc(0.99)/10.0;
    abo_lat = abort_latencies[i].perc(0.99)/10.0;
    of_data_accounting << "p99" << " " << exe_succ_lat << " " << exe_fail_lat << " " << val_succ_lat << " " << val_fail_lat << " " << com_lat << " " << abo_lat << std::endl;

    // avg
    exe_succ_lat = exe_succ_latencies[i].avg()/10.0;
    exe_fail_lat = exe_fail_latencies[i].avg()/10.0;
    val_succ_lat = validate_succ_latencies[i].avg()/10.0;
    val_fail_lat = validate_fail_latencies[i].avg()/10.0;
    com_lat = commit_latencies[i].avg()/10.0;
    abo_lat = abort_latencies[i].avg()/10.0;
    of_data_accounting << "avg" << " " << exe_succ_lat << " " << exe_fail_lat << " " << val_succ_lat << " " << val_fail_lat << " " << com_lat << " " << abo_lat << std::endl;
  }
#elif TX_PHASE_LATENCY == 0 && DATA_ACCOUNTING == 1
  Latency total_rw_hash_latency;
  Latency total_rw_val_latency;
  Latency total_ro_hash_latency;
  Latency total_ro_val_latency;
  for (int i=0; i<tx_names.size(); i++)
  {
    total_rw_hash_latency+=rw_hash_latency[i];
    total_rw_val_latency+=rw_val_latency[i];
    total_ro_hash_latency+=ro_hash_latency[i];
    total_ro_val_latency+=ro_val_latency[i];
    of_data_accounting << tx_names[i] << "_rw_latency hash val" << std::endl;
    of_data_accounting << "p50" << " " << rw_hash_latency[i].perc(0.5)/10.0 << " " << rw_val_latency[i].perc(0.5)/10.0 << std::endl;
    of_data_accounting << "p99" << " " << rw_hash_latency[i].perc(0.99)/10.0 << " " << rw_val_latency[i].perc(0.99)/10.0 << std::endl;
    of_data_accounting << "avg" << " " << rw_hash_latency[i].avg()/10.0 << " " << rw_val_latency[i].avg()/10.0 << std::endl;
    of_data_accounting << tx_names[i] << "_ro_latency hash val" << std::endl;
    of_data_accounting << "p50" << " " << ro_hash_latency[i].perc(0.5)/10.0 << " " << ro_val_latency[i].perc(0.5)/10.0 << std::endl;
    of_data_accounting << "p99" << " " << ro_hash_latency[i].perc(0.99)/10.0 << " " << ro_val_latency[i].perc(0.99)/10.0 << std::endl;
    of_data_accounting << "avg" << " " << ro_hash_latency[i].avg()/10.0 << " " << ro_val_latency[i].avg()/10.0 << std::endl;
  }
  of_data_accounting << "total_rw_latency hash val" << std::endl;
  of_data_accounting << "p50" << " " << total_rw_hash_latency.perc(0.5)/10.0 << " " << total_rw_val_latency.perc(0.5)/10.0 << std::endl;
  of_data_accounting << "p99" << " " << total_rw_hash_latency.perc(0.99)/10.0 << " " << total_rw_val_latency.perc(0.99)/10.0 << std::endl;
  of_data_accounting << "avg" << " " << total_rw_hash_latency.avg()/10.0 << " " << total_rw_val_latency.avg()/10.0 << std::endl;

  of_data_accounting << "total_ro_latency hash val" << std::endl;
  of_data_accounting << "p50" << " " << total_ro_hash_latency.perc(0.5)/10.0 << " " << total_ro_val_latency.perc(0.5)/10.0 << std::endl;
  of_data_accounting << "p99" << " " << total_ro_hash_latency.perc(0.99)/10.0 << " " << total_ro_val_latency.perc(0.99)/10.0 << std::endl;
  of_data_accounting << "avg" << " " << total_ro_hash_latency.avg()/10.0 << " " << total_ro_val_latency.avg()/10.0 << std::endl;
#elif TX_PHASE_LATENCY == 1 && DATA_ACCOUNTING == 1
  for (int i=0; i<tx_names.size(); i++)
  {
    of_data_accounting << tx_names[i] << " exec_succ_latency exec_fail_latency validate_succ_latency validate_fail_latency commit_latency abort_latency" << std::endl;
    // p50
    double exe_succ_lat = exe_succ_latencies[i].perc(0.5);
    double exe_fail_lat = exe_fail_latencies[i].perc(0.5);
    double val_succ_lat = validate_succ_latencies[i].perc(0.5)/10.0;
    double val_fail_lat = validate_fail_latencies[i].perc(0.5)/10.0;
    double com_lat = commit_latencies[i].perc(0.5)/10.0;
    double abo_lat = abort_latencies[i].perc(0.5)/10.0;
    of_data_accounting << std::fixed << std::setprecision(1) << "p50" << " " << exe_succ_lat << " " << exe_fail_lat << " " << val_succ_lat << " " << val_fail_lat << " " << com_lat << " " << abo_lat << std::endl;

    // p99
    exe_succ_lat = exe_succ_latencies[i].perc(0.99);
    exe_fail_lat = exe_fail_latencies[i].perc(0.99);
    val_succ_lat = validate_succ_latencies[i].perc(0.99)/10.0;
    val_fail_lat = validate_fail_latencies[i].perc(0.99)/10.0;
    com_lat = commit_latencies[i].perc(0.99)/10.0;
    abo_lat = abort_latencies[i].perc(0.99)/10.0;
    of_data_accounting << std::fixed << std::setprecision(1) << "p99" << " " << exe_succ_lat << " " << exe_fail_lat << " " << val_succ_lat << " " << val_fail_lat << " " << com_lat << " " << abo_lat << std::endl;

    // avg
    exe_succ_lat = exe_succ_latencies[i].avg();
    exe_fail_lat = exe_fail_latencies[i].avg();
    val_succ_lat = validate_succ_latencies[i].avg()/10.0;
    val_fail_lat = validate_fail_latencies[i].avg()/10.0;
    com_lat = commit_latencies[i].avg()/10.0;
    abo_lat = abort_latencies[i].avg()/10.0;
    of_data_accounting << std::fixed << std::setprecision(1) << "avg" << " " << exe_succ_lat << " " << exe_fail_lat << " " << val_succ_lat << " " << val_fail_lat << " " << com_lat << " " << abo_lat << std::endl;
  }

  Latency total_rw_hash_latency;
  Latency total_rw_val_latency;
  Latency total_ro_hash_latency;
  Latency total_ro_val_latency;
  Latency total_poll_cq_latency;
  for (int i=0; i<tx_names.size(); i++)
  {
    total_rw_hash_latency+=rw_hash_latency[i];
    total_rw_val_latency+=rw_val_latency[i];
    total_ro_hash_latency+=ro_hash_latency[i];
    total_ro_val_latency+=ro_val_latency[i];
    total_poll_cq_latency+=poll_cq_latency[i];
    of_data_accounting << tx_names[i] << "_rw_latency hash val" << std::endl;
    of_data_accounting << "p50" << " " << rw_hash_latency[i].perc(0.5)/10.0 << " " << rw_val_latency[i].perc(0.5)/10.0 << std::endl;
    of_data_accounting << "p99" << " " << rw_hash_latency[i].perc(0.99)/10.0 << " " << rw_val_latency[i].perc(0.99)/10.0 << std::endl;
    of_data_accounting << "avg" << " " << rw_hash_latency[i].avg()/10.0 << " " << rw_val_latency[i].avg()/10.0 << std::endl;
    of_data_accounting << tx_names[i] << "_ro_latency hash val" << std::endl;
    of_data_accounting << "p50" << " " << ro_hash_latency[i].perc(0.5)/10.0 << " " << ro_val_latency[i].perc(0.5)/10.0 << std::endl;
    of_data_accounting << "p99" << " " << ro_hash_latency[i].perc(0.99)/10.0 << " " << ro_val_latency[i].perc(0.99)/10.0 << std::endl;
    of_data_accounting << "avg" << " " << ro_hash_latency[i].avg()/10.0 << " " << ro_val_latency[i].avg()/10.0 << std::endl;
  }
  of_data_accounting << "total_rw_latency hash val" << std::endl;
  of_data_accounting << "p50" << " " << total_rw_hash_latency.perc(0.5)/10.0 << " " << total_rw_val_latency.perc(0.5)/10.0 << std::endl;
  of_data_accounting << "p99" << " " << total_rw_hash_latency.perc(0.99)/10.0 << " " << total_rw_val_latency.perc(0.99)/10.0 << std::endl;
  of_data_accounting << "avg" << " " << total_rw_hash_latency.avg()/10.0 << " " << total_rw_val_latency.avg()/10.0 << std::endl;

  of_data_accounting << "total_ro_latency hash val" << std::endl;
  of_data_accounting << "p50" << " " << total_ro_hash_latency.perc(0.5)/10.0 << " " << total_ro_val_latency.perc(0.5)/10.0 << std::endl;
  of_data_accounting << "p99" << " " << total_ro_hash_latency.perc(0.99)/10.0 << " " << total_ro_val_latency.perc(0.99)/10.0 << std::endl;
  of_data_accounting << "avg" << " " << total_ro_hash_latency.avg()/10.0 << " " << total_ro_val_latency.avg()/10.0 << std::endl;

  of_data_accounting << "total_poll_cq " << total_poll_cq_latency.count() << "times latency(ns)" << std::endl;
  of_data_accounting << std::fixed << std::setprecision(1) << "p50" << " " << total_poll_cq_latency.perc(0.5) << std::endl;
  of_data_accounting << std::fixed << std::setprecision(1) << "p99" << " " << total_poll_cq_latency.perc(0.99) << std::endl;
  of_data_accounting << std::fixed << std::setprecision(1) << "avg" << " " << total_poll_cq_latency.avg() << std::endl;
  of_data_accounting << std::endl;
#endif

  of_data_accounting.close();
}
