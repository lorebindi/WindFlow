/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *
 *  This file is part of WindFlow.
 *
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/master/LICENSE.MIT
 *
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

/**
 *  @file    source.hpp
 *  @author  Gabriele Mencagli
 *
 *  @brief Source operator
 *
 *  @section Source (Description)
 *
 *  This file implements the Source operator able to generate output streams
 */

#ifndef SOURCE_H
#define SOURCE_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<source_shipper.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include <ff/barrier.hpp>

using namespace ff;

namespace wf {

//@cond DOXY_IGNORE

// class Source_Replica
template<typename source_func_t>
class Source_Replica: public Basic_Replica
{
private:
    source_func_t func; // functional logic used by the Source replica
    using result_t = decltype(get_result_t_Source(func)); // extracting the result_t type and checking the admissible signatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), Source_Shipper<result_t> &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), Source_Shipper<result_t> &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched, "WindFlow Compilation Error - Source_Replica does not have a valid functional logic:\n");
    Time_Policy_t time_policy; // time policy of the Source replica
    Source_Shipper<result_t> *shipper; // pointer to the shipper object used by the Source replica to send outputs
    PinningSpinBarrier* barrier;
    bool has_barrier = false;

public:
    // Constructor
    Source_Replica(source_func_t _func,
                   std::string _opName,
                   RuntimeContext _context,
                   std::function<void(RuntimeContext &)> _closing_func):
                   Basic_Replica(_opName, _context, _closing_func, false),
                   func(_func),
                   barrier(nullptr),
                   time_policy(Time_Policy_t::INGRESS_TIME),
                   shipper(nullptr) {    }

    // Constructor
    Source_Replica(source_func_t _func,
                   std::string _opName,
                   RuntimeContext _context,
                   PinningSpinBarrier* _barrier,
                   bool _has_barrier,
                   std::function<void(RuntimeContext &)> _closing_func):
                   Basic_Replica(_opName, _context, _closing_func, false),
                   func(_func),
                   barrier(_barrier),
                   has_barrier(_has_barrier),
                   time_policy(Time_Policy_t::INGRESS_TIME),
                   shipper(nullptr) {    }

    // Copy Constructor
    Source_Replica(const Source_Replica &_other):
                   Basic_Replica(_other),
                   func(_other.func),
                   time_policy(_other.time_policy),
                   barrier(_other.barrier),
                   has_barrier(_other.has_barrier)
    {
        if (_other.shipper != nullptr) {
            shipper = new Source_Shipper<result_t>(*(_other.shipper));
            shipper->node = this; // change the node referred by the shipper
#if defined (WF_TRACING_ENABLED)
            shipper->setStatsRecord(&(this->stats_record)); // change the Stats_Record referred by the shipper
#endif
        }
        else {
            shipper = nullptr;
        }
    }

    // Destructor
    ~Source_Replica() override
    {
        if (shipper != nullptr) {
            delete shipper;
        }
    }

    // svc_init (utilized by the FastFlow runtime)
    int svc_init() override
    {
        //pinning
        if(context.getReplicaIndex()==0)
            ff_mapThreadToCpu(5);
        if(context.getReplicaIndex()==1)
            ff_mapThreadToCpu(21);
        if(context.getReplicaIndex()==1)
            ff_mapThreadToCpu(37);
        // Call the barrier if set
        if (this->get_has_barrier()) {
            barrier->doBarrier(opName, context.getReplicaIndex());  // Wait on the barrier
        }
        shipper->setInitialTime(current_time_usecs()); // set the initial time
        return Basic_Replica::svc_init();
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *) override
    {
        if constexpr (isNonRiched) { // non-riched version
            func(*shipper);
        }
        if constexpr (isRiched)  { // riched version
            func(*shipper, this->context);
        }
        this->closing_func(this->context); // call the closing function
        shipper->flush(); // call the flush of the shipper
        this->terminated = true;
#if defined (WF_TRACING_ENABLED)
        (this->stats_record).setTerminated();
#endif
        return this->EOS; // end-of-stream
    }

    // eosnotify (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override {} // no actions here (it is a Source)

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override {} // it must be empty now (closing_func already called in eosnotify)

    // Set the emitter used to route outputs generated by the Source replica
    void setEmitter(Basic_Emitter *_emitter) override
    {
        // if a shipper already exists, it is destroyed
        if (shipper != nullptr) {
            delete shipper;
        }
        shipper = new Source_Shipper<result_t>(_emitter, this, this->execution_mode, time_policy); // create the shipper
        shipper->setInitialTime(current_time_usecs()); // set the initial time
#if defined (WF_TRACING_ENABLED)
        shipper->setStatsRecord(&(this->stats_record));
#endif
    }

    // Set the execution and time mode of the Source replica
    void setConfiguration(Execution_Mode_t _execution_mode, Time_Policy_t _time_policy)
    {
        this->setExecutionMode(_execution_mode);
        time_policy = _time_policy;
        if (shipper != nullptr) {
            shipper->setConfiguration(_execution_mode, time_policy);
        }
    }

    bool get_has_barrier() {
        return has_barrier;
    }


    Source_Replica(Source_Replica &&) = delete; ///< Move constructor is deleted
    Source_Replica &operator=(const Source_Replica &) = delete; ///< Copy assignment operator is deleted
    Source_Replica &operator=(Source_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/**
 *  \class Source
 *
 *  \brief Source operator
 *
 *  This class implements the Source operator able to generate a stream of outputs
 *  all having the same type.
 */
template<typename source_func_t>
class Source: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    source_func_t func; // functional logic used by the Source
    using result_t = decltype(get_result_t_Source(func)); // extracting the result_t type and checking the admissible signatures
    std::vector<Source_Replica<source_func_t>*> replicas; // vector of pointers to the replicas of the Source
    static constexpr op_type_t op_type = op_type_t::SOURCE;
    PinningSpinBarrier* barrier;

    // Configure the Source to receive batches instead of individual inputs (cannot be called for the Source)
    void receiveBatches(bool _input_batching) override
    {
        abort(); // <-- this method cannot be used!
    }

    // Set the emitter used to route outputs from the Source
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Source has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode and the time policy of the Source
    void setConfiguration(Execution_Mode_t _execution_mode, Time_Policy_t _time_policy)
    {
        if (this->getOutputBatchSize() > 0 && _execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Source is trying to produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for(auto *r: replicas) {
            r->setConfiguration(_execution_mode, _time_policy);
        }
    }

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Source to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Source");
        writer.Key("Distribution");
        writer.String("NONE");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(false);
        writer.Key("isGPU");
        writer.Bool(false);
        writer.Key("Parallelism");
        writer.Uint(this->parallelism);
        writer.Key("OutputBatchSize");
        writer.Uint(this->outputBatchSize);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the Source
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.EndObject();
    }
#endif

public:
    /**
     *  \brief Constructor
     *
     *  \param _func functional logic of the Source (a function or any callable type)
     *  \param _parallelism internal parallelism of the Source
     *  \param _name name of the Source
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Source (a function or any callable type)
     */
    Source(source_func_t _func,
           size_t _parallelism,
           std::string _name,
           size_t _outputBatchSize,
           std::function<void(RuntimeContext &)> _closing_func):
           Basic_Operator(_parallelism, _name, Routing_Mode_t::NONE /* fixed to NONE for the Source */, _outputBatchSize),
           func(_func),
           barrier(nullptr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Source
            replicas.push_back(new Source_Replica<source_func_t>(_func, this->name, RuntimeContext(this->parallelism, i), _closing_func));
        }
    }

    /**
     *
    *   \param _func functional logic of the Source (a function or any callable type)
     *  \param _parallelism internal parallelism of the Source
     *  \param _name name of the Source
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _barrier spinBarrier used from Source_Replica, required for pinning
     *  \param _closing_func closing functional logic of the Source (a function or any callable type)
     */
    Source(source_func_t _func,
           size_t _parallelism,
           std::string _name,
           size_t _outputBatchSize,
           PinningSpinBarrier* _barrier,
           std::function<void(RuntimeContext &)> _closing_func):
           Basic_Operator(_parallelism, _name, Routing_Mode_t::NONE /* fixed to NONE for the Source */, _outputBatchSize),
           func(_func),
           barrier(_barrier)
    {
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Source
            auto replica =  new Source_Replica<source_func_t>(_func, this->name, RuntimeContext(this->parallelism, i), _barrier, true, _closing_func);
            replicas.push_back(replica);
        }
    }

    /// Copy constructor
    Source(const Source &_other):
           Basic_Operator(_other),
           func(_other.func),
           barrier(_other.barrier)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Source replicas
            replicas.push_back(new Source_Replica<source_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Source() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /**
     *  \brief Get the type of the Source as a string
     *  \return type of the Source
     */
    std::string getType() const override
    {
        return std::string("Source");
    }



    Source(Source &&) = delete; ///< Move constructor is deleted
    Source &operator=(const Source &) = delete; ///< Copy assignment operator is deleted
    Source &operator=(Source &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif