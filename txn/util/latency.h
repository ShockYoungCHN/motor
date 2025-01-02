// Author: Ming Zhang
// Adapted from mica
// Copyright (c) 2023
// Modified by: Yuanzhuo Yang

#pragma once

#include <algorithm>
#include <cstdio>

// Test ibv_poll_cq
static inline unsigned long GetCPUCycle() {
  unsigned a, d;
  __asm __volatile("rdtsc"
                   : "=a"(a), "=d"(d));
  return ((unsigned long)a) | (((unsigned long)d) << 32);
}

#include <iostream>
#include <cstring>
#include <cinttypes>
#include <cstdio>

class Latency
{
public:
  Latency() {
    initBins();
    reset();
  }

  void reset() {
    memset(bins_, 0, sizeof(bins_));
    overflow_bin_ = 0;
  }

  void update(size_t us) {
    int bin_index = -1;
    for (int i = 0; i < kNumBins; ++i) {
      if (us < bin_start_[i + 1]) {
        bin_index = i;
        break;
      }
    }
    if (bin_index != -1) {
      size_t index = (us - bin_start_[bin_index]) / step_[bin_index];
      bins_[bin_index][index]++;
    } else {
      overflow_bin_++;
    }
  }

  void update(size_t us, int count) {
    int bin_index = -1;
    for (int i = 0; i < kNumBins; ++i) {
      if (us < bin_start_[i + 1]) {
        bin_index = i;
        break;
      }
    }
    if (bin_index != -1) {
      size_t index = (us - bin_start_[bin_index]) / step_[bin_index];
      bins_[bin_index][index] += count;
    } else {
      overflow_bin_ += count;
    }
  }

  Latency& operator+=(const Latency& o) {
    for (int i = 0; i < kNumBins; ++i) {
      for (size_t j = 0; j < kBinSize; ++j) {
        bins_[i][j] += o.bins_[i][j];
      }
    }
    overflow_bin_ += o.overflow_bin_;
    return *this;
  }

  size_t count() const {
    size_t count = overflow_bin_;
    for (int i = 0; i < kNumBins; ++i) {
      for (size_t j = 0; j < kBinSize; ++j) {
        count += bins_[i][j];
      }
    }
    return count;
  }

  size_t sum() const {
    size_t sum = overflow_bin_ * bin_start_[kNumBins]; // overflow bin
    for (int i = 0; i < kNumBins; ++i) {
      size_t bin_base = bin_start_[i];
      size_t s = step_[i];
      for (size_t j = 0; j < kBinSize; ++j) {
        size_t val = bin_base + j * s;
        sum += bins_[i][j] * val;
      }
    }
    return sum;
  }

  double avg() const {
    size_t cnt = count();
    if (cnt == 0) return 0.0;
    return static_cast<double>(sum()) / cnt;
  }

  size_t min() const {
    for (int i = 0; i < kNumBins; ++i) {
      for (size_t j = 0; j < kBinSize; ++j) {
        if (bins_[i][j] != 0) {
          return bin_start_[i] + j * step_[i];
        }
      }
    }
    if (overflow_bin_ != 0) {
      return bin_start_[kNumBins];
    }
    return 0;
  }

  size_t max() const {
    if (overflow_bin_ != 0) {
      return bin_start_[kNumBins];
    }
    for (int i = kNumBins - 1; i >= 0; --i) {
      for (int j = kBinSize - 1; j >= 0; --j) {
        if (bins_[i][j] != 0) {
          return bin_start_[i] + j * step_[i];
        }
      }
    }
    return 0;
  }

  size_t perc(double p) const {
    size_t total = count();
    if (total == 0) return 0;
    int64_t thres = static_cast<int64_t>(p * total);
    for (int i = 0; i < kNumBins; ++i) {
      for (size_t j = 0; j < kBinSize; ++j) {
        thres -= bins_[i][j];
        if (thres < 0) {
          return bin_start_[i] + j * step_[i];
        }
      }
    }
    return bin_start_[kNumBins];
  }

  void print(FILE* fp) const {
    for (int i = 0; i < kNumBins; ++i) {
      for (size_t j = 0; j < kBinSize; ++j) {
        if (bins_[i][j] != 0) {
          fprintf(fp, "%5zu %6zu\n", bin_start_[i] + j * step_[i], bins_[i][j]);
        }
      }
    }
    if (overflow_bin_ != 0) {
      fprintf(fp, "%5zu %6zu\n", bin_start_[kNumBins], overflow_bin_);
    }
  }

private:
  static const int kNumBins = 12;
  static const int kBinSize = 128;

  size_t bins_[kNumBins][kBinSize];
  size_t overflow_bin_;

  size_t step_[kNumBins];
  size_t bin_start_[kNumBins + 1];

  void initBins() {
    step_[0] = 1;
    step_[1] = 2;
    step_[2] = 4;
    step_[3] = 8;
    step_[4] = 16;
    step_[5] = 16;
    step_[6] = 16;
    step_[7] = 16;
    step_[8] = 128;
    step_[9] = 128;
    step_[10] = 1024;
    step_[11] = 1024; // max 305024

    bin_start_[0] = 0;
    for (int i = 0; i < kNumBins; ++i) {
      bin_start_[i + 1] = bin_start_[i] + step_[i] * kBinSize;
    }
  }
};