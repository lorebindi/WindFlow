#ifndef PINNING_THREAD_CONTEXT_HPP
#define PINNING_THREAD_CONTEXT_HPP

#include <ff/barrier.hpp>
#include<vector>

using namespace std;
using namespace ff;

namespace wf {

    /**
     * pinning_thread_context is the shared struct between operators and used for realizing replicas' thread pinning.
     */
    struct pinning_thread_context {
        PinningSpinBarrier& barrier; // This spin barrier is used to ensure that threads only start working once all other threads have been pinned correctly.
        bool has_barrier;
        vector<int>& cores;  // This is the core vector on which the operator replicas will be pinned.

        pinning_thread_context(PinningSpinBarrier& b, bool active, vector<int>& c)
            : barrier(b), has_barrier(active), cores(c) {}
    };
}
#endif // PINNING_THREAD_CONTEXT_HPP
