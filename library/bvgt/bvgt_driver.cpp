//
// Created by sjf on 9/15/2022.
//

#include "bvgt_driver.h"

namespace gfe::library {

/*****************************************************************************
 *                                                                           *
 *  Init                                                                     *
 *                                                                           *
 *****************************************************************************/
    BVGTDriver::BVGTDriver(bool is_directed) : top_block(new SpruceTransVer::TopBlock),
                                               m_is_directed(is_directed), vertex_num(1),
    edge_num(0){
//        if (is_directed == true) { throw std::invalid_argument("Graph is directed"); }
//        else {
////            throw std::invalid_argument("Graph is undirected");
//        }
    }

    BVGTDriver::~BVGTDriver() {

    }

    void BVGTDriver::on_thread_init(int thread_id) {
//        throw NotImplemented();
    }

    void BVGTDriver::on_thread_destroy(int thread_id) {

    }

    void* BVGTDriver::handle_impl() {
        return top_block;
    }

/*****************************************************************************
 *                                                                           *
 *  Updates & point look ups                                                 *
 *                                                                           *
 *****************************************************************************/

    uint64_t BVGTDriver::num_edges() const {
        return edge_num;
    }

    uint64_t BVGTDriver::num_vertices() const {
        return vertex_num;
    }

    bool BVGTDriver::has_vertex(uint64_t vertex_id) const {
        return true;
    }

    double BVGTDriver::get_weight(uint64_t source, uint64_t destination) const {
        return 0.0;
    }

    bool BVGTDriver::is_directed() const {
        return m_is_directed;
    }

    void BVGTDriver::set_timeout(uint64_t seconds) {
//        m_timeout = chrono::seconds{seconds};
    }

    void BVGTDriver::set_thread_affinity(bool value) {

    }

    bool BVGTDriver::has_thread_affinity() const {
        return true;
    }

    bool BVGTDriver::has_read_only_transactions() const {
        return true;
    }

    bool BVGTDriver::add_vertex(uint64_t vertex_id) {
        // using add_vertex to change vertex_num
        vertex_num = vertex_id;
        return true;
    }

    bool BVGTDriver::remove_vertex(uint64_t vertex_id) {
//        COUT_DEBUG("remove vertex: " << vertex_id);
        return true;
    }

    bool BVGTDriver::add_edge(gfe::graph::WeightedEdge e) {
        edge_num++;
        return SpruceTransVer::InsertEdge((SpruceTransVer::TopBlock*)top_block, SpruceTransVer::WeightedEdge{(uint32_t)e.m_source, (uint32_t)e.m_destination, (float)e.m_weight});
    }

    bool BVGTDriver::add_edge_v2(gfe::graph::WeightedEdge e) {
        edge_num++;
        // for graph analysis
       if (e.m_destination >= vertex_num) vertex_num = e.m_destination + 1;
       if (e.m_source >= vertex_num) vertex_num = e.m_source + 1;

//       if(!is_directed()){
        SpruceTransVer::InsertEdge((SpruceTransVer::TopBlock*)top_block, SpruceTransVer::WeightedEdge{(uint32_t)e.m_source, (uint32_t)e.m_destination, (float)e.m_weight});
        SpruceTransVer::InsertEdge((SpruceTransVer::TopBlock*)top_block, SpruceTransVer::WeightedEdge{(uint32_t)e.m_destination, (uint32_t)e.m_source, (float)e.m_weight});
//        }
//       else {
//           SpruceTransVer::InsertEdge((SpruceTransVer::TopBlock*)top_block, SpruceTransVer::WeightedEdge{(uint32_t)e.m_source, (uint32_t)e.m_destination, (float)e.m_weight});
//       }
        return true;
    }

    bool BVGTDriver::update_edge(gfe::graph::WeightedEdge e) {
//        std::cout << "Updateing Edge..." << std::endl;

//        {
//            //only for mixed workloads
//            if (e.m_destination >= vertex_num) {vertex_num = e.m_destination + 1; std::cout <<vertex_num << std::endl;}
//            if (e.m_source >= vertex_num) {vertex_num = e.m_source + 1; std::cout <<vertex_num << std::endl; }
//        }
        SpruceTransVer::UpdateEdge((SpruceTransVer::TopBlock*)top_block, SpruceTransVer::WeightedEdge{(uint32_t)e.m_destination, (uint32_t)e.m_source, (float)e.m_weight});
        return SpruceTransVer::UpdateEdge((SpruceTransVer::TopBlock*)top_block, SpruceTransVer::WeightedEdge{(uint32_t)e.m_source, (uint32_t)e.m_destination, (float)e.m_weight});
//        return true;
    }

    bool BVGTDriver::remove_edge(gfe::graph::Edge e) {
        edge_num--;
        SpruceTransVer::DeleteEdge((SpruceTransVer::TopBlock*)top_block, (uint32_t)e.m_source, (uint32_t)e.m_destination);
        SpruceTransVer::DeleteEdge((SpruceTransVer::TopBlock*)top_block, (uint32_t)e.m_destination, (uint32_t)e.m_source);
        return true;
    }

/*****************************************************************************
 *                                                                           *
 *  Dump                                                                     *
 *                                                                           *
 *****************************************************************************/

    void BVGTDriver::dump_ostream(std::ostream &out) const {

    }


/*****************************************************************************
 *                                                                           *
 *  Helpers                                                                  *
 *                                                                           *
 *****************************************************************************/
// Translate the logical into real vertices IDs. Materialization step at the end of a Graphalytics algorithm


//    template<typename T, bool negative_scores = true>
//    void BVGTDriver::save_results(std::vector<std::pair<uint64_t, T>> &result, const char* dump2file) {
//
//    }

/*****************************************************************************
 *                                                                           *
 *  BFS                                                                      *
 *                                                                           *
 *****************************************************************************/
//    namespace { // anonymous

/*
GAP Benchmark Suite
Kernel: Breadth-First Search (BFS)
Author: Scott Beamer
Will return parent array for a BFS traversal from a source vertex
This BFS implementation makes use of the Direction-Optimizing approach [1].
It uses the alpha and beta parameters to determine whether to switch search
directions. For representing the frontier, it uses a SlidingQueue for the
top-down approach and a Bitmap for the bottom-up approach. To reduce
false-sharing for the top-down approach, thread-local QueueBuffer's are used.
To save time computing the number of edges exiting the frontier, this
implementation precomputes the degrees in bulk at the beginning by storing
them in parent array as negative numbers. Thus the encoding of parent is:
  parent[x] < 0 implies x is unvisited and parent[x] = -out_degree(x)
  parent[x] >= 0 implies x been visited
[1] Scott Beamer, Krste Asanović, and David Patterson. "Direction-Optimizing
    Breadth-First Search." International Conference on High Performance
    Computing, Networking, Storage and Analysis (SC), Salt Lake City, Utah,
    November 2012.
*/


        void BVGTDriver::bfs(uint64_t source_vertex_id, const char* dump2file) {
        std::vector<uint32_t> neighbours;
        SpruceTransVer::get_neighbours_only((SpruceTransVer::TopBlock*)top_block, source_vertex_id, neighbours);
        DOBFS((SpruceTransVer::TopBlock*)top_block, (uint32_t)source_vertex_id, vertex_num, edge_num, neighbours.size());
        }

/*****************************************************************************
 *                                                                           *
 *  PageRank                                                                 *
 *                                                                           *
 *****************************************************************************/

// Implementation based on the reference PageRank for the GAP Benchmark Suite
// https://github.com/sbeamer/gapbs
// The reference implementation has been written by Scott Beamer
//
// Copyright (c) 2015, The Regents of the University of California (Regents)
// All Rights Reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. Neither the name of the Regents nor the
//    names of its contributors may be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL REGENTS BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

/*
GAP Benchmark Suite
Kernel: PageRank (PR)
Author: Scott Beamer

Will return pagerank scores for all vertices once total change < epsilon

This PR implementation uses the traditional iterative approach. This is done
to ease comparisons to other implementations (often use same algorithm), but
it is not necesarily the fastest way to implement it. It does perform the
updates in the pull direction to remove the need for atomics.
*/




        void BVGTDriver::pagerank(uint64_t num_iterations, double damping_factor, const char* dump2file) {
            PageRankPull((SpruceTransVer::TopBlock*)top_block, num_iterations, vertex_num);
        }

/*****************************************************************************
 *                                                                           *
 *  WCC                                                                      *
 *                                                                           *
 *****************************************************************************/
// Implementation based on the reference WCC for the GAP Benchmark Suite
// https://github.com/sbeamer/gapbs
// The reference implementation has been written by Scott Beamer
//
// Copyright (c) 2015, The Regents of the University of California (Regents)
// All Rights Reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. Neither the name of the Regents nor the
//    names of its contributors may be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL REGENTS BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


//#define DEBUG_WCC
/*
GAP Benchmark Suite
Kernel: Connected Components (CC)
Author: Scott Beamer

Will return comp array labelling each vertex with a connected component ID

This CC implementation makes use of the Shiloach-Vishkin [2] algorithm with
implementation optimizations from Bader et al. [1]. Michael Sutton contributed
a fix for directed graphs using the min-max swap from [3], and it also produces
more consistent performance for undirected graphs.

[1] David A Bader, Guojing Cong, and John Feo. "On the architectural
    requirements for efficient execution of graph algorithms." International
    Conference on Parallel Processing, Jul 2005.

[2] Yossi Shiloach and Uzi Vishkin. "An o(logn) parallel connectivity algorithm"
    Journal of Algorithms, 3(1):57–67, 1982.

[3] Kishore Kothapalli, Jyothish Soman, and P. J. Narayanan. "Fast GPU
    algorithms for graph connectivity." Workshop on Large Scale Parallel
    Processing, 2010.
*/

// The hooking condition (comp_u < comp_v) may not coincide with the edge's
// direction, so we use a min-max swap such that lower component IDs propagate
// independent of the edge's direction.

        void BVGTDriver::wcc(const char* dump2file) {
            ShiloachVishkin((SpruceTransVer::TopBlock*)top_block, vertex_num);
        }

/*****************************************************************************
 *                                                                           *
 *  CDLP                                                                     *
 *                                                                           *
 *****************************************************************************/
// same impl~ as the one done for llama



        void BVGTDriver::cdlp(uint64_t max_iterations, const char* dump2file) {
            cdlp_m((SpruceTransVer::TopBlock*)top_block, (uint32_t)max_iterations, (uint32_t)vertex_num);
        }

/*****************************************************************************
 *                                                                           *
 *  LCC                                                                      *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG_LCC
#if defined(DEBUG_LCC)
#define COUT_DEBUG_LCC(msg) COUT_DEBUG(msg)
#else
#define COUT_DEBUG_LCC(msg)
#endif

// loosely based on the impl~ made for Stinger



        void BVGTDriver::lcc(const char* dump2file) {
            OrderedCount((SpruceTransVer::TopBlock*)top_block, vertex_num);
        }

/*****************************************************************************
 *                                                                           *
 *  SSSP                                                                     *
 *                                                                           *
 *****************************************************************************/
// Implementation based on the reference SSSP for the GAP Benchmark Suite
// https://github.com/sbeamer/gapbs
// The reference implementation has been written by Scott Beamer
//
// Copyright (c) 2015, The Regents of the University of California (Regents)
// All Rights Reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. Neither the name of the Regents nor the
//    names of its contributors may be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL REGENTS BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


        void BVGTDriver::sssp(uint64_t source_vertex_id, const char* dump2file) {
            DeltaStep((SpruceTransVer::TopBlock*)top_block, source_vertex_id, 2.0, vertex_num, edge_num);
        }

/*****************************************************************************
 *                                                                           *
 *  LCC, sort-merge implementation                                           *
 *                                                                           *
 *****************************************************************************/
/**
 * Algorithm parameters
 */

//    } // namespace
}