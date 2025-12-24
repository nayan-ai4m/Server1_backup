/* Copyright 2019-2020 Baumer Optronic */
#include <string>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <vector>
#include <mutex>
#include "TimingStatistic.h"

TimingStat::TimingStat() {
    hist_times_ = {
        5, 10, 15, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 150, 200, 300,
        400, 500, 750, 1000, 2000, 3000, 4000, 5000, 7500, 10000 };
    Reset();
    Start();
}

TimingStat::~TimingStat() {
}

void TimingStat::Reset() {
    std::lock_guard<std::mutex> lock(statistic_lock_);
    count_ = 0;
    sum_ = 0;
    min_ = 0;
    max_ = 0;
    last_ = 0;

    // resize to one more than times to hold overflow
    hist_cnts_.resize(hist_times_.size() + 1);
    std::fill(hist_cnts_.begin(), hist_cnts_.end(), 0);
}

// start the timer from a prior time
void TimingStat::Start(std::chrono::steady_clock::time_point start_tp) {
    start_ = start_tp;
}

// start the timer now
uint64_t TimingStat::Start() {
    std::chrono::steady_clock::time_point start_tp = std::chrono::steady_clock::now();
    Start(start_tp);
    return start_tp.time_since_epoch().count();
}

// end the timer now, add to intev
void TimingStat::Stop(std::chrono::steady_clock::time_point end_tp) {
    end_ = end_tp;
    std::chrono::duration<double, std::milli> ms = end_ - start_;
    Add_ms(ms.count());
}

uint64_t TimingStat::Stop() {
    std::chrono::steady_clock::time_point stop_tp = std::chrono::steady_clock::now();
    Stop(stop_tp);
    return stop_tp.time_since_epoch().count();
}

// adds ms value to the statistics
void TimingStat::Add_ms(double new_ms) {
    std::lock_guard<std::mutex> lock(statistic_lock_);
    last_ = new_ms;
    if (count_ > 0) {
        count_++;
        sum_ += last_;
        min_ = std::min(last_, min_);
        max_ = std::max(last_, max_);
    } else {
        count_ = 1;
        sum_ = last_;
        min_ = last_;
        max_ = last_;
    }

    // collect histogram of times
    int scan_ms = static_cast<int>(last_ + 0.5);
    int bin = 0;
    for (; bin < static_cast<int>(hist_times_.size()); bin++) {
        if (scan_ms <= hist_times_[bin]) {
            hist_cnts_[bin]++;
            break;
        }
    }
    // over last threashold, store in overflow bin
    if (bin >= static_cast<int>(hist_times_.size())) {
        hist_cnts_[hist_times_.size()]++;
    }
}

// formats interval statistics:
//  last   count   ave     min  ---  histogram  ---   max  (max-min)
// 9.2ms   12345 x 8.3ms   1.1< 340 <5< 12000 <10< 5 <12.3 (11.2)
//
// In the 'histogram' the interval bounds are next to the '<' and '<'
//   and the counts in the intervals are surrounded by spaces
// In the above example:
//   340 timings were above 1.1ms and less/equal to 5ms
//   12000 timings were above 5ms and less/equal to 10ms
//   5 timings were above 10ms and less/equal to 12.3ms
std::string TimingStat::Hist() {
    std::lock_guard<std::mutex> lock(statistic_lock_);
    std::string stats;
    if (count_ <= 0) {
        stats = "0 ms";
    } else {
        std::stringstream buf;
        int digits = 1;
        double ave = Ave_ms();
        if (count_ > 0 && ave > 0.0 && ave < .06) {
            digits = 3;
        }
        double fps = 0;
        if (ave > .0001) {
            fps = 1000 / ave;
        }
        buf.setf(std::ios::fixed, std::ios::floatfield);  // set fixed floating format
        buf.precision(digits);  // for fixed format, decimal places
        buf << last_;
        buf << "  " << count_ << " x " << Ave_ms() << "ms(";
        buf.precision(1);
        buf << fps << "/s)  " << min_ << "<";
        // find last used bin
        int last_bin = static_cast<int>(hist_cnts_.size()) - 1;
        for (; last_bin >= 0; last_bin--) {
            if (hist_cnts_[last_bin]) {
                break;
            }
        }
        // find first used bin
        int bin = 0;
        for (; bin < last_bin; bin++) {
            if (hist_cnts_[bin]) {
                break;
            }
        }
        // show used bins
        for (; bin < last_bin; bin++) {
            if (hist_cnts_[bin] || hist_cnts_[bin + 1]) {
                buf << " " << hist_cnts_[bin] << " <" << hist_times_[bin] << "<";
            }
        }
        buf << " " << hist_cnts_[bin] << " <" << max_;
        double range = max_ - min_;
        digits = (range > 0.0 && range < .06) ? 3 : 1;
        buf.precision(digits);
        buf << " (" << range << ")";
        stats = buf.str();
    }
    return stats;
}
