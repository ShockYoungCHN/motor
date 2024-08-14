// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <list>
#include <util/debug.h>

#include "base/common.h"
#include "rlib/logging.hpp"
#include "rlib/rdma_ctrl.hpp"
#include "scheduler/coroutine.h"

using namespace rdmaio;

// Scheduling coroutines. Each txn thread only has ONE scheduler
class CoroutineScheduler {
 public:
  // The coro_num includes all the coroutines
  CoroutineScheduler(t_id_t thread_id, coro_id_t coro_num) {
    t_id = thread_id;
    pending_qps = std::vector<RCQP*>(MAX_REMOTE_NODE_NUM, nullptr);
    pending_counts = new int[coro_num];
    for (coro_id_t c = 0; c < coro_num; c++) {
      pending_counts[c] = 0;
    }
    coro_array = new Coroutine[coro_num];
  }

  ~CoroutineScheduler() {
    if (pending_counts) {
      delete[] pending_counts;
    }

    if (coro_array) {
      delete[] coro_array;
    }
  }

  // For RDMA requests
  void AddPendingQP(coro_id_t coro_id, RCQP* qp);

  void AddPendingLogQP(coro_id_t coro_id, RCQP* qp);

  void RDMABatch(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int piggyback_num);

  bool RDMABatchSync(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int piggyback_num);

  bool RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size);

  bool RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size, MemoryAttr& local_mr, MemoryAttr& remote_mr);

  void RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size);

  bool RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size, MemoryAttr& local_mr, MemoryAttr& remote_mr);

  bool RDMAReadInv(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size);

  bool RDMAReadSync(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size);

  bool RDMACAS(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap);

  bool RDMAMaskedCAS(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap, uint64_t compare_mask, uint64_t swap_mask);

  // For polling
  void PollCompletion(t_id_t tid);  // There is a coroutine polling ACKs

  // Link coroutines in a loop manner
  void LoopLinkCoroutine(coro_id_t coro_num);

  // For coroutine yield, used by transactions
  void Yield(coro_yield_t& yield, coro_id_t cid);

  // Append this coroutine to the tail of the yield-able coroutine list
  // Used by coroutine 0
  void AppendCoroutine(Coroutine* coro);

  // Start this coroutine. Used by coroutine 0
  void RunCoroutine(coro_yield_t& yield, Coroutine* coro);

 public:
  Coroutine* coro_array;

  Coroutine* coro_head;

  Coroutine* coro_tail;

  long long dur0 = 0, dur1 = 0, dur2 = 0;
 private:
  t_id_t t_id;

  // node_id: RCQP*
  std::vector<RCQP*> pending_qps;

  // number of pending qps (i.e., the ack has not received) per coroutine
  int* pending_counts;
  int pending_counts_sum = 0;
};

ALWAYS_INLINE
void CoroutineScheduler::AddPendingQP(coro_id_t coro_id, RCQP* qp) {
  if (unlikely(pending_qps[qp->idx_.node_id] == nullptr)) {
    pending_qps[qp->idx_.node_id] = qp;
  }
  pending_qps[qp->idx_.node_id]->low_watermark_++;
  pending_counts[coro_id] += 1;
  pending_counts_sum += 1;
}

ALWAYS_INLINE
void CoroutineScheduler::RDMABatch(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int piggyback_num) {
  // piggyback_num should be 1 less than the total number of batched reqs
  send_sr[piggyback_num].wr_id = coro_id;
  auto rc = qp->post_batch(send_sr, bad_sr_addr);
  if (rc != SUCC) {
    RDMA_LOG(FATAL) << "client: post batch fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
  }
  AddPendingQP(coro_id, qp);
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMABatchSync(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int piggyback_num) {
  send_sr[piggyback_num].wr_id = coro_id;
  auto rc = qp->post_batch(send_sr, bad_sr_addr);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post batch fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll batch fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size, MemoryAttr& local_mr, MemoryAttr& remote_mr) {
  auto rc = qp->post_send_to_mr(local_mr, remote_mr, IBV_WR_RDMA_WRITE, wt_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
void CoroutineScheduler::RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(FATAL) << "client: post read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
  }
  AddPendingQP(coro_id, qp);
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size, MemoryAttr& local_mr, MemoryAttr& remote_mr) {
  auto rc = qp->post_send_to_mr(local_mr, remote_mr, IBV_WR_RDMA_READ, rd_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAReadInv(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, 0, 0);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAReadSync(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMACAS(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap) {
  auto rc = qp->post_cas(local_buf, remote_offset, compare, swap, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAMaskedCAS(coro_id_t coro_id,
                                       RCQP* qp,
                                       char* local_buf,
                                       uint64_t remote_offset,
                                       uint64_t compare,
                                       uint64_t swap,
                                       uint64_t compare_mask,
                                       uint64_t swap_mask) {
  auto rc = qp->post_masked_cas(local_buf, remote_offset, compare, swap, compare_mask, swap_mask, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

// Link coroutines in a loop manner
ALWAYS_INLINE
void CoroutineScheduler::LoopLinkCoroutine(coro_id_t coro_num) {
  // The coroutines are maintained in an array,
  // but linked via pointers for efficient yield scheduling
  for (uint i = 0; i < coro_num; ++i) {
    coro_array[i].prev_coro = coro_array + i - 1;
    coro_array[i].next_coro = coro_array + i + 1;
  }
  coro_head = &(coro_array[0]);
  coro_tail = &(coro_array[coro_num - 1]);
  coro_array[0].prev_coro = coro_tail;
  coro_array[coro_num - 1].next_coro = coro_head;
}

// For coroutine yield, used by transactions
ALWAYS_INLINE
void CoroutineScheduler::Yield(coro_yield_t& yield, coro_id_t cid) {
  if (unlikely(pending_counts[cid] == 0)) {
    return;
  }
  // 1. Remove this coroutine from the yield-able (yield-able=ready to run) coroutine list
  Coroutine* coro = &coro_array[cid];
  assert(coro->is_wait_poll == false);
  Coroutine* next = coro->next_coro;
  coro->prev_coro->next_coro = next;
  next->prev_coro = coro->prev_coro;
  if (coro_tail == coro) coro_tail = coro->prev_coro;
  coro->is_wait_poll = true;
  // 2. Yield to the next coroutine
  // RDMA_LOG(DBG) << "coro: " << cid << " yields to coro " << next->coro_id;
  RunCoroutine(yield, next);
}

// Start this coroutine. Used by coroutine 0 and Yield()
ALWAYS_INLINE
void CoroutineScheduler::RunCoroutine(coro_yield_t& yield, Coroutine* coro) {
  // RDMA_LOG(DBG) << "yield to coro: " << coro->coro_id;
  coro->is_wait_poll = false;
  yield(coro->func);
}

// Append this coroutine to the tail of the yield-able coroutine list. Used by coroutine 0
ALWAYS_INLINE
void CoroutineScheduler::AppendCoroutine(Coroutine* coro) {
  if (!coro->is_wait_poll) return;
  Coroutine* prev = coro_tail;
  prev->next_coro = coro;
  coro_tail = coro;
  coro_tail->next_coro = coro_head; // todo: don't u need to set coro_head->prev_coro = coro_tail?
  coro_tail->prev_coro = prev;
}

inline long long now() {
  auto now_time_point = std::chrono::high_resolution_clock::now();

  auto now_in_ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(now_time_point);

  return now_in_ns.time_since_epoch().count();
}

ALWAYS_INLINE
void CoroutineScheduler::PollCompletion(t_id_t tid)
{
  // long long dur1 = 0, dur2 = 0;
  int max_completions = pending_counts_sum;
  ibv_wc wc[max_completions];

  do
  {
    for (int i = 0; i < pending_qps.size(); i++)
    {
      // auto t3_1 = now();
      if (pending_qps[i] == nullptr || pending_qps[i]->low_watermark_ == 0)
        continue;
      // auto t3_2 = now();
      auto curr_cq = pending_qps[i]->cq_;
      int poll_result = ibv_poll_cq(curr_cq, max_completions, wc);
      if (poll_result == 0) {
        continue;
      }
      pending_qps[i]->low_watermark_ -= poll_result;
      pending_counts_sum -= poll_result;

      for (int j = 0; j < poll_result; j++) {
        if (unlikely(wc[j].status != IBV_WC_SUCCESS)) {
          TLOG(INFO, tid) << "Bad completion status: " << wc[j].status << " with error " << ibv_wc_status_str(wc[j].status);
          if (wc[j].status != IBV_WC_RETRY_EXC_ERR) {
            TLOG(INFO, tid) << "completion status != IBV_WC_RETRY_EXC_ERR. abort()";
            abort();
          }
          continue;
        }
        auto coro_id = wc[j].wr_id;
        assert(pending_counts[coro_id] > 0);
        pending_counts[coro_id] -= 1;
        if (pending_counts[coro_id] == 0) {
          AppendCoroutine(&coro_array[coro_id]);
        }
      }
      // dur1 += t3_2 - t3_1;
      // dur2 += now() - t3_2;
    }
  } while (coro_head->next_coro->coro_id == 0);
  // this->dur1 += dur1;
  // this->dur2 += dur2;
}