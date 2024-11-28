//
// Created by Stardust on 2023/6/25.
//

#ifndef GRAPHINDEX_SPRUCE_TRANSACTION_H
#define GRAPHINDEX_SPRUCE_TRANSACTION_H

#define BLOCK_THRESHOLD 20
#define BLOCK_SIZE 16
#define DELETE_TYPE 0
#define UPDATE_TYPE 1

#include "header.h"

class SpruceTransVer{
public:

    typedef struct _top_block{
        uint8_t bitmap_8kb[1<<13];
        std::atomic<uint64_t> ptr_to_children[1<<16];
        std::atomic<uint8_t> mtx[1<<10];
    } TopBlock;

    typedef struct _middle_block{
        uint8_t bitmap_8kb[1<<13];
        std::atomic<uint64_t> ptr_to_children[1<<10];
        std::atomic<uint8_t> mtx[1<<10];
        std::atomic<uint8_t> obsolete_flag;
    } MiddleBlock;

    typedef struct _weighted_edge {
        uint32_t src;
        uint32_t des;
        float weight;
    } WeightedEdge;

    typedef struct _weighted_out_edge {
        uint32_t des;
        uint32_t timestamp;
        double weight;
//        float weight_2;
//        uint64_t timestamp;
    } WeightedOutEdge;

    typedef struct _des_t {
        uint32_t des;
        uint32_t timestamp;
    }DesT;

    typedef struct _weighted_out_edge_simple {
        uint32_t des;
        double weight;
    } WeightedOutEdgeSimple;

    typedef struct _versioned_out_edge {
        uint64_t timestamp;     //implicitly use the high 8 bit to indicate type
        struct _weighted_out_edge ver;
    }VersionedOutEdge;

    typedef struct _log_block {
        // sequentially stores version
        VersionedOutEdge versioned_edges[BLOCK_SIZE];
        struct _log_block* next_block;
    }LogBlock;


//    //May not need;
//    typedef uint64_t* PtrToPtrBlock;

    // 32B/block

    typedef struct _ptr_block {
        std::atomic<uint8_t> obsolete_flag;
        std::atomic<uint64_t> ptr_to_buffer[64];
        std::atomic<uint8_t> buffer_locks[64];
    } PtrBlock;

    typedef struct _adj_subsequent_block_one {
        std::atomic<uint8_t> type;
        std::atomic<uint8_t> obsolete_flag;
        std::atomic<uint16_t> log_size;
        std::atomic<uint64_t> timestamp;
        std::atomic<uint64_t> bitvector_64;
        std::atomic<uint64_t> ptr_to_log_block;
        WeightedOutEdge adj_vertex[4];
    }AdjSubsequentBlockOne;

    typedef struct _adj_subsequent_block_two {
        std::atomic<uint8_t> type;
        std::atomic<uint8_t> obsolete_flag;
        std::atomic<uint16_t> log_size;
        std::atomic<uint64_t> timestamp;
        std::atomic<uint64_t> bitvector_64;
        std::atomic<uint64_t> ptr_to_log_block;
        WeightedOutEdge adj_vertex[8];
    }AdjSubsequentBlockTwo;

    typedef struct _adj_subsequent_block_three {
        std::atomic<uint8_t> type;
        std::atomic<uint8_t> obsolete_flag;
        std::atomic<uint16_t> log_size;
        std::atomic<uint64_t> timestamp;
        std::atomic<uint64_t> bitvector_64;
        std::atomic<uint64_t> ptr_to_log_block;
        WeightedOutEdge adj_vertex[16];
    }AdjSubsequentBlockThree;

    typedef struct _adj_subsequent_block_four {
        std::atomic<uint8_t> type;
        std::atomic<uint8_t> obsolete_flag;
        std::atomic<uint16_t> log_size;
        std::atomic<uint64_t> timestamp;
        std::atomic<uint64_t> bitvector_64;
        std::atomic<uint64_t> ptr_to_log_block;
        WeightedOutEdge adj_vertex[32];
    }AdjSubsequentBlockFour;

    typedef struct _adj_subsequent_block_five {
        std::atomic<uint8_t> type;
        std::atomic<uint8_t> obsolete_flag;
        std::atomic<uint16_t> log_size;
        std::atomic<uint64_t> timestamp;
        std::atomic<uint64_t> bitvector_64;
        std::atomic<uint64_t> ptr_to_log_block;
        WeightedOutEdge adj_vertex[64];
        std::atomic<uint64_t> next_block;
    }AdjSubsequentBlockFive;

    inline static bool CompareOutEdges (WeightedOutEdgeSimple a, WeightedOutEdgeSimple b) {
        return a.des < b.des;
    }

    static void ReadGraphToVector(const std::string &graph_path, std::vector<WeightedEdge> &edges, bool undirected_flag = 0, bool weight_flag = 0);

    static void InsertEdgeFromVector(TopBlock* root, std::vector<WeightedEdge> &edges, bool shuffle_flag = 0);

    static void ClearStatistics();

    static void PrintStatistics();

    static uint32_t GetDegree(SpruceTransVer::TopBlock* root, const uint32_t from_node_id);

    inline static TopBlock* CreateTopBlock() {
        auto root = (SpruceTransVer::TopBlock*) malloc(sizeof(SpruceTransVer::TopBlock));
        memset(root, 0, sizeof(SpruceTransVer::TopBlock));
        return root;
    }

    static TopBlock* BuildTree(const std::string &graph_path, bool undirected_flag = 0, bool weight_flag = 0);

    static bool InsertEdge(TopBlock* root, WeightedEdge edge);

    static bool get_neighbours(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
                               std::vector<WeightedOutEdgeSimple> &neighbours /*tbb::concurrent_vector<int> &neighbours*/);

    static bool get_neighbours_only(TopBlock* root, const uint32_t from_node_id, std::vector<uint32_t> &neighbours);

    static bool get_neighbours_sorted_only(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
                                           std::vector<uint32_t> &neighbours);

    static bool get_neighbours_exclusively(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
            /*tbb::concurrent_vector<int> &neighbours*/std::vector<WeightedOutEdgeSimple> &neighbours);

    static bool DeleteEdge(TopBlock* root, const uint32_t from_node_id, const uint32_t to_node_id);

    static bool get_neighbours_sorted(TopBlock* root, const uint32_t from_node_id, std::vector<WeightedOutEdgeSimple> &neighbours);

    static bool UpdateEdge(TopBlock* root, WeightedEdge edge);

    static bool AddVersion(uint8_t type, SpruceTransVer::AdjSubsequentBlockFive* local_head_block_ptr, SpruceTransVer::WeightedOutEdge edge);

    static uint64_t get_global_timestamp();

    static bool get_neighbours_snapshot(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
                                                 std::vector<WeightedOutEdgeSimple> &neighbours);

    static bool
    get_neighbours_only_exclusively(TopBlock* root, const uint32_t from_node_id, std::vector<uint32_t> &neighbours);
};

#include <bitset>
#include <vector>
#include <memory>

typedef struct _edge_property {
    double weight;
    uint64_t timestamp;
} EdgeProperty;

class EdgeTree {
public:

    EdgeTree();

    class Node {
    public:
        std::bitset<256> bitmap;
        virtual ~Node() = default;
    };

    class InternalNode : public Node {
    public:
        std::vector<std::unique_ptr<Node>> children;
    };

    class LeafNode : public Node {
    public:
        std::vector<EdgeProperty> values;
    };

    void insert(uint32_t key, EdgeProperty value);
    void get_kvs(std::vector<SpruceTransVer::WeightedOutEdgeSimple>& results);
    std::vector<EdgeProperty> get(uint32_t key);

    inline uint8_t extractKeySegment(uint32_t key, int level) {
        return (key >> ((4 - level) * 8)) & 0xFF;
    }

    inline uint64_t get_64bit_chunk(const std::bitset<256>& bitset, int chunk_index) {
        uint64_t chunk = 0;
        for (int i = 0; i < 64; ++i) {
            if (bitset[chunk_index * 64 + i]) {
                chunk |= 1ULL << i;
            }
        }
        return chunk;
    }

    inline unsigned popcount_upto_index(const std::bitset<256>& bitmap, uint8_t index) {
        unsigned count = 0;

        for (int i = 0; i < index / 64; ++i) {
            count += __builtin_popcountll(get_64bit_chunk(bitmap, i));
        }

        // Handle the remaining bits
        count += __builtin_popcountll(get_64bit_chunk(bitmap, index / 64) & ((1ULL << (index % 64)) - 1));

        return count;
    }

private:
    std::unique_ptr<Node> root;
    std::unique_ptr<Node> createNode();
    std::unique_ptr<LeafNode> createLeafNode();
    void insertHelper(Node* node, uint32_t key, EdgeProperty value, int level);
    void getAllKeysAndValues(Node* node, uint32_t currentKey, int level, std::vector<SpruceTransVer::WeightedOutEdgeSimple>& results);
    std::vector<EdgeProperty> getHelper(Node* node, uint32_t key, int level);
};
#endif //GRAPHINDEX_SPRUCE_TRANSACTION_H
