//
// Created by sjf on 9/15/2022.
//

#ifndef GRAPHINDEX_BVGT_DRIVER_H
#define GRAPHINDEX_BVGT_DRIVER_H

#include <chrono>
#include <utility>
#include <vector>
#include "library/interface.hpp"
#include "spruce_transaction.h"
#include "cdlp.h"
#include "header.h"
#include "./third_party/gapbs/src/pr_spmv.h"
#include "./third_party/gapbs/src/bfs.h"
#include "./third_party/gapbs/src/tc.h"
#include "./third_party/gapbs/src/sssp.h"
#include "./third_party/gapbs/src/cc_sv.h"

namespace gfe::library {

    class BVGTDriver : public virtual UpdateInterface, public virtual GraphalyticsInterface {

    protected:
        void* top_block; // pointer to the library
        const bool m_is_directed; // whether the underlying graph is directed or undirected
        std::chrono::seconds m_timeout{0}; // the budget to complete each of the algorithms in the Graphalytics suite
        std::atomic<uint32_t> vertex_num;
        std::atomic<uint32_t> edge_num;
        // Helper, save the content of the vector to the given output file
//        template<typename T, bool negative_scores = true>
//    void save_results(std::vector<std::pair<uint64_t, T>>& result, const char* dump2file);
    public:
        /**
         * Create an instance of Spruce
         * @param is_directed: whether the underlying graph should be directed or undirected
         * @param read_only: whether to use read-only transactions for the algorithms in graphalytics
         */


        BVGTDriver(bool is_directed);

        /**
         * Destructor
         */
        virtual ~BVGTDriver();

        /**
         * Dump the content of the graph to given stream
         */
        void dump_ostream(std::ostream &out) const;

        /**
         * Get the number of edges contained in the graph
         */
        virtual uint64_t num_edges() const;

        /**
         * Get the number of nodes stored in the graph
         */
        virtual uint64_t num_vertices() const;

        /**
         * Returns true if the given vertex is present, false otherwise
         */
        virtual bool has_vertex(uint64_t vertex_id) const;

        /**
         * Returns the weight of the given edge is the edge is present, or NaN otherwise
         */
        virtual double get_weight(uint64_t source, uint64_t destination) const;

        /**
         * Check whether the graph is directed
         */
        virtual bool is_directed() const;

        /**
         * Impose a timeout on each graph computation. A computation that does not terminate by the given seconds will raise a TimeoutError.
         */
        virtual void set_timeout(uint64_t seconds);

        /**
         * Whether to restrict the execution of the OpenMP threads in sockets
         */
        virtual void set_thread_affinity(bool value);

        /**
         * Whether thread affinity is set
         */
        virtual bool has_thread_affinity() const;

        /**
         * Whether Graphalytics transactions should be `read-only'
         */
        virtual bool has_read_only_transactions() const;

        /**
         * Add the given vertex to the graph
         * @return true if the vertex has been inserted, false otherwise (that is, the vertex already exists)
         */
        virtual bool add_vertex(uint64_t vertex_id);

        /**
         * Remove the mapping for a given vertex. The actual internal vertex is not removed from the adjacency list.
         * @param vertex_id the vertex to remove
         * @return true if a mapping for that vertex existed, false otherwise
         */
        virtual bool remove_vertex(uint64_t vertex_id);

        /**
         * Add the given edge in the graph
         * @return true if the edge has been inserted, false if this edge already exists or one of the referred
         *         vertices does not exist.
         */
        virtual bool add_edge(gfe::graph::WeightedEdge e);

        /**
         * Add the given edge in the graph. Implicitly create the referred vertices if they do not already exist
         * @return true if the edge has been inserted, false otherwise (e.g. this edge already exists)
         */
        virtual bool add_edge_v2(gfe::graph::WeightedEdge e);

        virtual bool update_edge(gfe::graph::WeightedEdge e);

        /**
         * Remove the given edge from the graph
         * @return true if the given edge has been removed, false otherwise (e.g. this edge does not exist)
         */
        virtual bool remove_edge(gfe::graph::Edge e);

        /**
         * Callback, invoked when a thread is created
         */
        virtual void on_thread_init(int thread_id);

        /**
         * Callback, invoked when a thread is going to be removed
         */
        virtual void on_thread_destroy(int thread_id);

        /**
         * Perform a BFS from source_vertex_id to all the other vertices in the graph.
         * @param source_vertex_id the vertex where to start the search
         * @param dump2file if not null, dump the result in the given path, following the format expected by the benchmark specification
         */
        virtual void bfs(uint64_t source_vertex_id, const char* dump2file = nullptr);

        /**
         * Execute the PageRank algorithm for the specified number of iterations.
         *
         * @param num_iterations the number of iterations to execute the algorithm
         * @param damping_factor weight for the PageRank algorithm, it affects the score associated to the sink nodes in the graphs
         * @param dump2file if not null, dump the result in the given path, following the format expected by the benchmark specification
         */
        virtual void pagerank(uint64_t num_iterations, double damping_factor = 0.85, const char* dump2file = nullptr);

        /**
         * Weakly connected components (WCC), associate each node to a connected component of the graph
         * @param dump2file if not null, dump the result in the given path, following the format expected by the benchmark specification
         */
        virtual void wcc(const char* dump2file = nullptr);

        /**
         * Community Detection using Label-Propagation. Associate a label to each vertex of the graph, according to its neighbours.
         * @param max_iterations max number of iterations to perform
         * @param dump2file if not null, dump the result in the given path, following the format expected by the benchmark specification
         */
        virtual void cdlp(uint64_t max_iterations, const char* dump2file = nullptr);

        /**
         * Local clustering coefficient. Associate to each vertex the ratio between the number of its outgoing edges and the number of
         * possible remaining edges.
         * @param dump2file if not null, dump the result in the given path, following the format expected by the benchmark specification
         */
        virtual void lcc(const char* dump2file = nullptr);

        /**
         * Single-source shortest paths. Compute the weight related to the shortest path from the source to any other vertex in the graph.
         * @param source_vertex_id the vertex where to start the search
         * @param dump2file if not null, dump the result in the given path, following the format expected by the benchmark specification
         */
        virtual void sssp(uint64_t source_vertex_id, const char* dump2file = nullptr);

        /**
         * Retrieve the handle to the Teseo implementation
         */
        void* handle_impl();
    };
}

#endif //GRAPHINDEX_BVGT_DRIVER_H
