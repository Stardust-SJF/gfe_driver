/**
 * Copyright (C) 2019 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#pragma once

#include <memory>
#include <vector>

// forward declarations
namespace common { class Database; }
namespace experiment { class Aging2Experiment; }
namespace experiment::details { class Aging2Master; }
namespace experiment::details { class Aging2Worker; }

namespace experiment {

class Aging2Result {
    friend class details::Aging2Master;
    friend class details::Aging2Worker;

    const uint64_t m_num_threads; // the total number of threads used for the experiment, that is, the parallelism degree
    const uint64_t m_worker_granularity = 1024; // the granularity of a task for a worker, that is the number of contiguous operations (inserts/deletes) performed inside the threads between each invocation to the scheduler.
    uint64_t m_num_artificial_vertices = 0; // the total number of artificial vertices (not present in the loaded graph) inserted during the updates
    uint64_t m_completion_time = 0; // the amount of time to complete all updates, in microsecs
    uint64_t m_num_vertices_load = 0; // the number of vertices loaded from the input graph
    uint64_t m_num_vertices_final_graph = 0; // the number of vertices in the final graph, after all updates have been performed
    uint64_t m_num_edges_load = 0; // the number of edges loaded from the input graph
    uint64_t m_num_edges_final_graph = 0; // the number of edges in the final graph, after all updates have been performed
    uint64_t m_num_build_invocations = 0; // total number of invocations to the method #build
    uint64_t m_num_operations_expected = 0; // total number of operations expected to be performed by the workers, before running the experiment
    uint64_t m_num_operations_performed = 0; // total number of operations performed by the workers
    std::vector<uint64_t> m_reported_times; // time to complete 1x, 2x, 3x, ... updates (inserts/deletions) w.r.t. the size of the input graph, in microsecs

public:
    // Default ctor
    Aging2Result(const Aging2Experiment& parameters);

    // Destructor
    ~Aging2Result();

    // the total number of artificial vertices (not present in the loaded graph) inserted during the updates
    uint64_t num_artificial_vertices() const {
        return m_num_artificial_vertices;
    }

    // Save the results of the experiment into the database
    void save(common::Database* db);
    void save(std::shared_ptr<common::Database> db);
};

} // namespace
