#ifndef PINNING_THREAD_CONTEXT_HPP
#define PINNING_THREAD_CONTEXT_HPP

#include <ff/barrier.hpp>
#include<vector>

using namespace std;
using namespace ff;

namespace wf {

    struct pinning_thread_context {
        PinningSpinBarrier& barrier;
        bool has_barrier;
        vector<int>& cores;

        pinning_thread_context(PinningSpinBarrier& b, bool active, vector<int>& c)
            : barrier(b), has_barrier(active), cores(c) {}
    };

}

#endif // PINNING_THREAD_CONTEXT_HPP
