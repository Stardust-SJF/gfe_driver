//
// Created by Stardust on 2023/6/25.
//

#include "spruce_transaction.h"

extern std::atomic<int> type1, type2, type3, type4, type5, type64, edge_array_num;
extern std::atomic<int> middle_block_num_, dense_ptr_block_num_, sparse_ptr_block_num, second_block_num_, third_block_num_, fourth_block_num_, adj_head_num_, adj_subsequent_block_num_;
extern std::atomic<double> total_insert_time;
extern std::atomic<uint64_t> global_timestamp;

void
SpruceTransVer::ReadGraphToVector(const std::string &graph_path, std::vector<WeightedEdge> &edges, bool undirected_flag,
                              bool weight_flag) {
    edges.clear();
    //Open file
    std::ifstream initial_graph(graph_path, std::ios::in);
    if (!initial_graph) {
        std::cout << "File Error!! " << std::endl;
        return;
    }

    std::string temp_line;
    int cnt = 0;
    std::string hyperlink = "Hyperlink";
    std::string twitter_2010 = "twitter-2010";
    std::string graph500 = "graph500";
    if (graph_path.find(twitter_2010) != std::string::npos || graph_path.find("uniform")!= std::string::npos || graph_path.find(hyperlink) != std::string::npos || graph_path.find(graph500) != std::string::npos) {

    } else {
        while (cnt++ < 4) {
            getline(initial_graph, temp_line);
            std::cout << temp_line << std::endl;
        }
    }

    // Read Graph Data (Normal)
    uint32_t from_node_id, to_node_id;

    cnt = 0;
    SpruceTransVer::WeightedEdge buffer_edge;
    srand(time(NULL));
    // randomly generate weights
    if (!undirected_flag) {
        while (initial_graph >> from_node_id) {
            initial_graph >> to_node_id;
            buffer_edge = {from_node_id, to_node_id, (float)rand()/RAND_MAX};
            edges.push_back(buffer_edge);
        }
    }
    else {
        while (initial_graph >> from_node_id) {
            initial_graph >> to_node_id;
            buffer_edge = {from_node_id, to_node_id, (float)rand()/RAND_MAX};
            edges.emplace_back(buffer_edge);
            buffer_edge = {to_node_id, from_node_id, (float)rand()/RAND_MAX};
            edges.emplace_back(buffer_edge);
        }
    }

    initial_graph.close();
}

void SpruceTransVer::InsertEdgeFromVector(SpruceTransVer::TopBlock* root, std::vector<WeightedEdge> &edges, bool shuffle_flag) {
    if (shuffle_flag) {
        std::shuffle(edges.begin(), edges.end(), std::mt19937(std::random_device()()));
    }
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
#pragma omp parallel for schedule(guided) num_threads(NUM_THREADS)
    for (int i = 0; i < edges.size(); i++) {
        SpruceTransVer::InsertEdge(root, edges[i]);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    total_insert_time += (end.tv_sec - start.tv_sec) * 1000000000 + end.tv_nsec - start.tv_nsec;
}

void SpruceTransVer::ClearStatistics() {
    global_timestamp = 0;
    middle_block_num_ = dense_ptr_block_num_ = sparse_ptr_block_num = adj_subsequent_block_num_ = adj_head_num_ = 0;
    type1 = type2 = type3 = type4 = type5 = type64 = edge_array_num = 0;
    total_insert_time = 0;
}

void SpruceTransVer::PrintStatistics() {
    std::cout << "Num of middle blocks: " << middle_block_num_ << std::endl;
    std::cout << "Num of sparse pointer blocks: " << sparse_ptr_block_num << std::endl;
    std::cout << "Num of dense pointer blocks: " << dense_ptr_block_num_ << std::endl;
    std::cout << "Num of adj head blocks: " << adj_head_num_ << std::endl;

    std::cout << "Num of adj type 1 subsequent blocks: " << type1 << std::endl;
    std::cout << "Num of adj type 2 subsequent blocks: " << type2 << std::endl;
    std::cout << "Num of adj type 3 subsequent blocks: " << type3 << std::endl;
    std::cout << "Num of adj type 4 subsequent blocks: " << type4 << std::endl;
    std::cout << "Num of adj type 5 subsequent blocks: " << type5 << std::endl;
    std::cout << "Num of edge arrays: " << edge_array_num << std::endl;
    std::cout << "Num of adj type 64 edge arrays: " << type64 << std::endl;

    std::cout << "Real Insertion time:" << total_insert_time / 1000000000 << "s" << std::endl;

    //actually size may be little smaller; not exactly data structure space

    long int index_space = sizeof(SpruceTransVer::TopBlock) + sizeof(SpruceTransVer::MiddleBlock) * middle_block_num_ +
                           sizeof(uint64_t) * sparse_ptr_block_num * (64 + 1) + sizeof(uint64_t) * dense_ptr_block_num_ * 32;
    long int edge_space = sizeof(SpruceTransVer::AdjSubsequentBlockOne) * type1 +
                          sizeof(SpruceTransVer::AdjSubsequentBlockTwo) * type2 + sizeof(AdjSubsequentBlockThree) * type3 +
                          sizeof(AdjSubsequentBlockFour) * type4 + (sizeof(SpruceTransVer::AdjSubsequentBlockFive)) * type5
                          +sizeof(WeightedOutEdge) * (64) * type64 + edge_array_num * 2 * sizeof(uint32_t);
    long int edge_array_size = sizeof(WeightedOutEdge) * (64) * type64 + edge_array_num * 2 * sizeof(uint32_t);
    long int analytical_space =
            sizeof(SpruceTransVer::TopBlock) + sizeof(SpruceTransVer::MiddleBlock) * middle_block_num_ +
            sizeof(uint64_t) * sparse_ptr_block_num * (64 + 1) + sizeof(uint64_t) * dense_ptr_block_num_ * 32 +
            //            sizeof(SpruceTransVer::AdjHeadBlock) * adj_head_num_ +
            sizeof(SpruceTransVer::AdjSubsequentBlockOne) * type1 +
            sizeof(SpruceTransVer::AdjSubsequentBlockTwo) * type2 + sizeof(AdjSubsequentBlockThree) * type3 +
            sizeof(AdjSubsequentBlockFour) * type4 + (sizeof(SpruceTransVer::AdjSubsequentBlockFive)) * type5
            +sizeof(WeightedOutEdge) * (64) * type64 + edge_array_num * 2 * sizeof(uint32_t);
    index_space = index_space / 1024;
    edge_space = edge_space / 1024;
    analytical_space = analytical_space / 1024;
    edge_array_size = edge_array_size / 1024;

    std::cout << "Index space: " << index_space << "KB" << std::endl;
    std::cout << "Edge space: " << edge_space << "KB" << std::endl;
    std::cout << "Edge Array space: " << edge_array_size << "KB" << std::endl;
    std::cout << "Analytical space: " << analytical_space << "KB" << std::endl;
}

SpruceTransVer::TopBlock* SpruceTransVer::BuildTree(const std::string &graph_path, bool undirected_flag, bool weight_flag) {
    //Init Tree
    auto root = (SpruceTransVer::TopBlock*) malloc(sizeof(SpruceTransVer::TopBlock));
    memset(root, 0, sizeof(SpruceTransVer::TopBlock));

    SpruceTransVer::ClearStatistics();
    auto g = SpruceTransVer::CreateTopBlock();
    std::vector<WeightedEdge> edges;
    SpruceTransVer::ReadGraphToVector(graph_path, edges, undirected_flag);
    SpruceTransVer::InsertEdgeFromVector(g, edges, 1);
    SpruceTransVer::PrintStatistics();
    return g;
}

bool SpruceTransVer::InsertEdge(SpruceTransVer::TopBlock* root, SpruceTransVer::WeightedEdge edge) {
    auto from_node_id = edge.src;
    auto to_node_id = edge.des;
    auto weight = edge.weight;

    //set up lock status
    int unlocked = UNLOCKED, read_locked = READ_LOCKED, write_locked = WRITE_LOCKED;
    uint8_t unlocked_m = UNLOCKED;
    uint8_t write_locked_m = WRITE_LOCKED;
    auto from_node_id_low = (uint16_t)from_node_id;
    auto from_node_id_high = (uint16_t)(from_node_id >> 16);
    //need lock: in case for deletion
    SpruceTransVer::MiddleBlock* middle_block_ptr;
    restart:
    //
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        //Noted that when thread get lock, corresponding middle block may be created, thereby recheck is needed;
        //try to get write lock (spin lock)
        while(!root->mtx[from_node_id_high/64].compare_exchange_strong(unlocked_m, write_locked_m)) {
            unlocked_m = UNLOCKED;
        }
        if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
            // Need to malloc new middle block
            middle_block_ptr = (SpruceTransVer::MiddleBlock*) malloc(sizeof(SpruceTransVer::MiddleBlock));
            memset(middle_block_ptr, 0, sizeof(SpruceTransVer::MiddleBlock));
            root->ptr_to_children[from_node_id_high].store((uint64_t)middle_block_ptr);
            set_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high);
            //Analysis
            middle_block_num_++;
        }
        else {
            //atomically load
            uint64_t temp = root->ptr_to_children[from_node_id_high].load();
            middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp;
        }
        root->mtx[from_node_id_high/64]--/*.store(UNLOCKED)*/;
    }
    else {
        //read atomically
        uint64_t temp = root->ptr_to_children[from_node_id_high].load();
        middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp;
    }

    // Get to middle block
    // in case for deletion
    if (!middle_block_ptr) {
        goto restart;
    }
    if (middle_block_ptr->obsolete_flag) {
        goto restart;
    }
    //now we need to get corresponding uint64_t and check the number of 1
    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
//    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t)(from_node_id_low % 64);

    //Decide the ptr type due to the number of 1

    SpruceTransVer::AdjSubsequentBlockOne* bottom_head_block;

    int lock_flag = 0;
    unlocked = UNLOCKED;

    if (!get_bit(&auxiliary_64, index_in_64)) {
        // does not exist, lock;
        while(!middle_block_ptr->mtx[ptr_block_index].compare_exchange_strong(unlocked_m, write_locked_m)) {
            unlocked_m = UNLOCKED;
        }
        lock_flag = 1;
    }




    //Get bottom head block
    // change set bit sequence for parallel: firstly edit pointer, then set bit
    // reget values!!!!
    auxiliary_64 = auxiliary_ptr[ptr_block_index];
    auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    SpruceTransVer::PtrBlock* ptr_block;
    // recheck
    if (!get_bit(&auxiliary_64, index_in_64)) {
        //bottom block does not exist, malloc a new block
        bottom_head_block = (SpruceTransVer::AdjSubsequentBlockOne*) malloc(sizeof(SpruceTransVer::AdjSubsequentBlockOne));
        memset(bottom_head_block, 0, sizeof(SpruceTransVer::AdjSubsequentBlockOne));
        bottom_head_block->bitvector_64 = UINT64_MAX;
        bottom_head_block->type.store(1);

        //Edit middle block bitmap and ptr block
        uint64_t temp = middle_block_ptr->ptr_to_children[ptr_block_index].load();
        ptr_block = (SpruceTransVer::PtrBlock*)temp;

        if(!ptr_block) {
            // + 1 for obsolete flag
            auto new_ptr_block = (SpruceTransVer::PtrBlock*) malloc(sizeof(SpruceTransVer::PtrBlock)) ;
            memset(new_ptr_block, 0, sizeof(SpruceTransVer::PtrBlock));
            new_ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<unsigned long>(bottom_head_block));
            middle_block_ptr->ptr_to_children[ptr_block_index].store((uint64_t)new_ptr_block);
            ptr_block = new_ptr_block;
            sparse_ptr_block_num++;
        }
        else {
            if (ptr_block->obsolete_flag) {
                //obsoleted
                if (lock_flag) {
                    middle_block_ptr->mtx[ptr_block_index]--;
                }
                goto restart;
            }
            ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<unsigned long>(bottom_head_block));
        }


        set_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t) from_node_id_low);
        //Analysis
//        adj_head_num_++;
        type1++;
    } else {
        //corresponding block exists
        uint64_t temp = middle_block_ptr->ptr_to_children[ptr_block_index].load();
        ptr_block = (SpruceTransVer::PtrBlock*)temp;
//        bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());
    }



    unlocked = UNLOCKED;
    if (lock_flag) {
        middle_block_ptr->mtx[ptr_block_index]--;
    }


    uint32_t insert_index;
    uint8_t type;

    while(!ptr_block->buffer_locks[index_in_64].compare_exchange_strong(unlocked_m, write_locked_m)) {
        unlocked_m = UNLOCKED;
    }
    auto local_head_block_ptr = (SpruceTransVer::AdjSubsequentBlockFive*)ptr_block->ptr_to_buffer[index_in_64].load();
    if (!local_head_block_ptr) {
        ptr_block->buffer_locks[index_in_64]--;
        goto restart;
    }
    if (local_head_block_ptr->obsolete_flag) {
        ptr_block->buffer_locks[index_in_64]--;
        goto restart;
    }
    //Edit timestamp: Note that for simplicity, only get it when reaching the vertex
    local_head_block_ptr->timestamp = SpruceTransVer::get_global_timestamp();
    //head insertion
    WeightedOutEdge out_edge = {edge.des, (uint32_t)local_head_block_ptr->timestamp , edge.weight};
    //Edit for SpruceTransVer

    if (local_head_block_ptr->bitvector_64 != 0) {
        //not full yet
        uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
        insert_index = __builtin_clzl(temp_bv_rev);
        type = local_head_block_ptr->type;
        clear_bit(&local_head_block_ptr->bitvector_64, insert_index);
        uint64_t temp;
        if (insert_index > ((1 << (type - 1)) * 4 - 1)) {
            //need to malloc new space;
            switch (type) {
                case 1: {
                    auto new_block = (SpruceTransVer::AdjSubsequentBlockTwo*) malloc(
                            sizeof(AdjSubsequentBlockTwo));
                    memset(new_block, 0, sizeof(AdjSubsequentBlockTwo));
                    temp = (uint64_t)local_head_block_ptr;
                    memcpy(new_block,
                           (SpruceTransVer::AdjSubsequentBlockOne*)(temp),
                           sizeof(SpruceTransVer::AdjSubsequentBlockOne));
                    new_block->adj_vertex[insert_index] = out_edge;
                    new_block->type.store(2);
                    local_head_block_ptr->obsolete_flag = 1;
                    // add previous block to delete queue
                    ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<uint64_t>(new_block));
                    free((SpruceTransVer::AdjSubsequentBlockOne*)(temp));
                    local_head_block_ptr = (SpruceTransVer::AdjSubsequentBlockFive*)new_block;
                    //analysis
                    type2++;
                    type1--;

                    break;
                }
                case 2: {
                    auto new_block = (SpruceTransVer::AdjSubsequentBlockThree*) malloc(
                            sizeof(AdjSubsequentBlockThree));
                    memset(new_block, 0, sizeof(AdjSubsequentBlockThree));
                    temp = (uint64_t)local_head_block_ptr;
                    memcpy(new_block,
                           (SpruceTransVer::AdjSubsequentBlockTwo*)temp,
                           sizeof(SpruceTransVer::AdjSubsequentBlockTwo));
                    new_block->adj_vertex[insert_index] = out_edge;
                    new_block->type.store(3);
                    local_head_block_ptr->obsolete_flag = 1;
                    // add previous block to delete queue
                    ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<uint64_t>(new_block));
                    free((SpruceTransVer::AdjSubsequentBlockTwo*)temp);
                    //analysis
                    type3++;
                    type2--;

                    break;
                }
                case 3: {
                    auto new_block = (SpruceTransVer::AdjSubsequentBlockFour*) malloc(
                            sizeof(AdjSubsequentBlockFour));
                    memset(new_block, 0, sizeof(AdjSubsequentBlockFour));
                    temp = (uint64_t)local_head_block_ptr;
                    memcpy(new_block,
                           (SpruceTransVer::AdjSubsequentBlockThree*)temp,
                           sizeof(SpruceTransVer::AdjSubsequentBlockThree));
                    new_block->adj_vertex[insert_index] = out_edge;
                    new_block->type.store(4);
                    local_head_block_ptr->obsolete_flag = 1;
                    // add previous block to delete queue
                    ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<uint64_t>(new_block));
                    free((SpruceTransVer::AdjSubsequentBlockThree*)temp);
                    //analysis
                    type4++;
                    type3--;

                    break;

                }
                case 4: {
                    auto new_block = (SpruceTransVer::AdjSubsequentBlockFive*) malloc(
                            sizeof(AdjSubsequentBlockFive));
                    temp = (uint64_t)local_head_block_ptr;
                    memset(new_block, 0, sizeof(AdjSubsequentBlockFive));
                    // new_block->bitvector_64 = UINT64_MAX; //no subsequent block
                    memcpy(new_block,
                           (SpruceTransVer::AdjSubsequentBlockFour*)(temp),
                           sizeof(SpruceTransVer::AdjSubsequentBlockFour));
                    new_block->adj_vertex[insert_index] = out_edge;
                    new_block->type.store(5);
                    local_head_block_ptr->obsolete_flag = 1;
                    // add previous block to delete queue
                    ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<uint64_t>(new_block));
                    free((SpruceTransVer::AdjSubsequentBlockFour*)temp);
                    //analysis
                    type5++;
                    type4--;

                    break;
                }
                default: {
                    std::cout << "Impossible type!" << std::endl;
                    break;
                }

            }
        } else {
            local_head_block_ptr->adj_vertex[insert_index] = out_edge;
        }
    } else {
        type = 5;
        // while ends, no spaces
        // Firstly check the block size,we assume that we only check invalid label in deletion
        uint32_t old_block_size, old_delete_num;
        uint64_t temp1 = (uint64_t)local_head_block_ptr;
        uint64_t temp2 = local_head_block_ptr->next_block.load();
        auto old_edge_array = (uint32_t*)temp2;

//        EdgeTree* edge_tree;
        // edge tree version
//        if (!old_edge_array) {
//            edge_tree = new EdgeTree();
//            local_head_block_ptr->next_block.store((uint64_t)edge_tree);
//        }
//        else {
//            edge_tree = (EdgeTree*)temp2;
//        }
//
//        for (int i = 0; i < 64; i++) {
//            edge_tree->insert(local_head_block_ptr->adj_vertex[i].des, {double(local_head_block_ptr->adj_vertex[i].weight), local_head_block_ptr->adj_vertex[i].timestamp});
//        }


        if (!old_edge_array) {
            // does not exist, set initial size

            old_block_size = 0;
            old_delete_num = 0;
            // analysis
            edge_array_num++;
        }
        else {
            old_block_size = old_edge_array[0];
            old_delete_num = old_edge_array[1];
        }

        auto old_edges = (SpruceTransVer::WeightedOutEdge*)(old_edge_array + 2);

        int old_delete_64 = 0;
        if (old_delete_num > 64) {
            old_delete_64 = old_delete_num/64;
        }

        // + 1 for block_size and delete_num (equals to weighted out edge)
        // resize block according to delete_num
        auto new_block_size = (64 + 1 + old_block_size - old_delete_64 * 64);
        auto new_edge_array = (uint32_t*) malloc(
                sizeof(SpruceTransVer::WeightedOutEdge) * (new_block_size));
        memset(new_edge_array, 0, sizeof(SpruceTransVer::WeightedOutEdge) * (new_block_size));
//

        //sort the vertex using bubble sort
        for (int i = 0; i < 64; i++) {
            bool flag = false;
            for (int j = 0; j < 64 - i - 1; j++) {
                if (local_head_block_ptr->adj_vertex[j].des > local_head_block_ptr->adj_vertex[j + 1].des) {
                    auto temp = local_head_block_ptr->adj_vertex[j];
                    local_head_block_ptr->adj_vertex[j] = local_head_block_ptr->adj_vertex[j + 1];
                    local_head_block_ptr->adj_vertex[j + 1] = temp;
                    flag = true;
                }
            }
            if (!flag) {
                break;
            }
        }

        //then use merge sort to place spaces in new block
        uint32_t k = 0;
        uint32_t start1 = 0, start2 = 0;
        uint32_t end1 = 64, end2 = old_block_size;

        auto new_edges = (SpruceTransVer::WeightedOutEdge*)(new_edge_array + 2);
        auto new_edge_info = (DesT*)(new_edges);
        auto new_property = (double*)(new_edge_info + (old_block_size + 64 - old_delete_64 * 64));
        auto old_edge_info = (DesT*)(old_edges);
        auto old_property = (double*)(old_edge_info + old_block_size);

        while (start1 < end1 && start2 < end2) {
            if (old_edge_info[start2].des == UINT32_MAX) {
                // skip invalid data
                start2++;
                continue;
            }
            if (local_head_block_ptr->adj_vertex[start1].des < old_edge_info[start2].des) {
                new_edge_info[k] = {local_head_block_ptr->adj_vertex[start1].des, local_head_block_ptr->adj_vertex[start1].timestamp};
                new_property[k++] = local_head_block_ptr->adj_vertex[start1++].weight;
            }
            else {
                new_edge_info[k] = old_edge_info[start2];
                new_property[k++] = old_property[start2++];
            }
//            new_edges[k++] = local_head_block_ptr->adj_vertex[start1].des < old_edges[start2].des ?
//                             local_head_block_ptr->adj_vertex[start1++] : old_edges[start2++];
        }
        while (start1 < end1) {
            new_edge_info[k] = {local_head_block_ptr->adj_vertex[start1].des, local_head_block_ptr->adj_vertex[start1].timestamp};
            new_property[k++] = local_head_block_ptr->adj_vertex[start1++].weight;
        }
        while (start2 < end2) {
            if (old_edge_info[start2].des == UINT32_MAX) {
                // skip invalid data
                start2++;
                continue;
            }
            new_edge_info[k] = old_edge_info[start2];
            new_property[k++] = old_property[start2++];
        }
        while (k < new_block_size - 1) {
            // shift invalid values with size < 64
            new_edge_info[k++].des = UINT32_MAX;
        }

//        //debug
//        if (new_block_size > 8000 && edge.src == 17) {
//            for (int i = 0; i < new_block_size; i++) {
//                std::cout <<new_edge_info[i].des << std::endl;
//            }
//        }




        //reset subsequent block status
        memset(local_head_block_ptr->adj_vertex, 0, sizeof(SpruceTransVer::WeightedOutEdge)*64);
        new_edge_array[0] = new_block_size - 1; // remember: block_size does not include first 2 uint32_t for information
        new_edge_array[1] = old_delete_num % 64;  // copy delete_num
        local_head_block_ptr->next_block.store(reinterpret_cast<uint64_t>(new_edge_array));
        //do not execute free function in new;
        // Noted that when execute on large datasets, use free function to avoid memory exceeded
        // when execute parallel test, comment it to avoid errors
        if(old_edge_array) free(old_edge_array);


        //reset head block status
        local_head_block_ptr->bitvector_64.store(UINT64_MAX);

        //Then insert new edge to subsequent block
        local_head_block_ptr->adj_vertex[0] = out_edge;
        clear_bit(&local_head_block_ptr->bitvector_64, 0);

        //analysis
        type64++;

    }
    ptr_block->buffer_locks[index_in_64]--;
    return true;
}

bool SpruceTransVer::get_neighbours(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
                                std::vector<WeightedOutEdgeSimple> &neighbours) {

    //do not use locks
    uint32_t restart_num = 0;
    restart:
    neighbours.clear();
    uint64_t temp_ptr;
    auto from_node_id_low = (uint16_t) from_node_id;
    auto from_node_id_high = (uint16_t) (from_node_id >> 16);
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        return false;
    }
    temp_ptr = root->ptr_to_children[from_node_id_high].load();
    auto* middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp_ptr;
    if (!middle_block_ptr) {
        return false;
    }
    if (!get_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t)from_node_id_low)) {
        return false;
    }
    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t) (from_node_id_low % 64);

    SpruceTransVer::AdjSubsequentBlockFive* bottom_head_block;

    SpruceTransVer::PtrBlock* ptr_block = (SpruceTransVer::PtrBlock*)middle_block_ptr->ptr_to_children[ptr_block_index].load();
    bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());

    if (!bottom_head_block) {
        return false;
    }

    SpruceTransVer::AdjSubsequentBlockFive* local_head_block_ptr = bottom_head_block;
    uint32_t get_index = 0;
    uint64_t temp_bitvector;

    recheck_bottom_lock:
    while (ptr_block->buffer_locks[index_in_64].load() != UNLOCKED);
    uint32_t timestamp_before = bottom_head_block->timestamp.load();
    // also recheck
    if (ptr_block->buffer_locks[index_in_64].load() == WRITE_LOCKED) {
        goto recheck_bottom_lock;
    }


    if (local_head_block_ptr->bitvector_64 != UINT64_MAX) {
        //value existed
        uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
        auto pre_bv = bottom_head_block->bitvector_64.load();
        auto type = local_head_block_ptr->type.load();
        if (type == 5) {
            //type = 5;
            //first check subsequent block
            for (int i = 0; i < 64; i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back({local_head_block_ptr->adj_vertex[i].des, local_head_block_ptr->adj_vertex[i].weight});
                }
            }

            //Edge tree version
//            temp_ptr = local_head_block_ptr->next_block.load();
//            auto edge_tree = (EdgeTree*)temp_ptr;
//            edge_tree->get_kvs(neighbours);


//            then check edge array
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            if (!edge_array) {
                block_size = 0;
                delete_space = 0;
            }
            else {
                block_size = edge_array[0];
                delete_space = edge_array[1];
            }
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(neighbours.size() + block_size - delete_space);
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);

            for (int i = 0; i < block_size; i++) {
                if (edge_info[i].des != UINT32_MAX) {
                    neighbours[new_index++] = {edge_info[i].des, property[i]};
                }
            }


        } else {
            //type = 4;
            for (int i = 0; i < (2 << type) ; i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back({local_head_block_ptr->adj_vertex[i].des, local_head_block_ptr->adj_vertex[i].weight});
                }
            }
        }
    } else {
        // check edge array
        if (!local_head_block_ptr->next_block.load()) {
            return false;
        }
        else {
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            block_size = edge_array[0];
            delete_space = edge_array[1];
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(neighbours.size() + block_size - delete_space);
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);

            for (int i = 0; i < block_size; i++) {
                if (edge_info[i].des != UINT32_MAX) {
                    neighbours[new_index++] = {edge_info[i].des, property[i]};
                }
            }
        }
    }
    uint32_t timestamp_after = bottom_head_block->timestamp.load();
    if (timestamp_before != timestamp_after) {
        if (restart_num < RESTART_THRESHOLD){
            restart_num++;
            goto restart;
        }
        else {
            // read bottom head block exclusively
            return get_neighbours_exclusively(root, from_node_id, neighbours);
        }
    }

    return true;
}

bool SpruceTransVer::get_neighbours_exclusively(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
                                            std::vector<WeightedOutEdgeSimple> &neighbours) {
    //do not use locks
    uint32_t restart_num = 0;
    restart:
    neighbours.clear();
    uint64_t temp_ptr;
    auto from_node_id_low = (uint16_t) from_node_id;
    auto from_node_id_high = (uint16_t) (from_node_id >> 16);
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        return false;
    }
    temp_ptr = root->ptr_to_children[from_node_id_high].load();
    auto* middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp_ptr;
    if (!middle_block_ptr) {
        return false;
    }
    if (!get_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t) from_node_id_low)) {
        return false;
    }
    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t) (from_node_id_low % 64);

    SpruceTransVer::AdjSubsequentBlockFive* bottom_head_block;

    SpruceTransVer::PtrBlock* ptr_block = (SpruceTransVer::PtrBlock*)middle_block_ptr->ptr_to_children[ptr_block_index].load();
    bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());

    if (!bottom_head_block) {
        return false;
    }

//    SpruceTransVer::AdjSubsequentBlockFive* local_subsequent_block_ptr;
    SpruceTransVer::AdjSubsequentBlockFive* local_head_block_ptr = bottom_head_block;
    uint32_t get_index = 0;
    uint64_t temp_bitvector;
    uint8_t unlocked_m = UNLOCKED;
    uint8_t write_locked_m = WRITE_LOCKED;
    while(!ptr_block->buffer_locks[index_in_64].compare_exchange_strong(unlocked_m, write_locked_m)) {
        unlocked_m = UNLOCKED;
    }

    if (local_head_block_ptr->bitvector_64 != UINT64_MAX) {
        //value existed
        uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
        auto pre_bv = bottom_head_block->bitvector_64.load();
        auto type = local_head_block_ptr->type.load();
        if (type == 5) {
            //type = 5;
            //first check subsequent block
            for (int i = 0; i < 64; i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back({local_head_block_ptr->adj_vertex[i].des, local_head_block_ptr->adj_vertex[i].weight});
                }
            }
            //then check edge array
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            if (!edge_array) {
                block_size = 0;
                delete_space = 0;
            }
            else {
                block_size = edge_array[0];
                delete_space = edge_array[1];
            }
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(neighbours.size() + block_size - delete_space);
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);

            for (int i = 0; i < block_size; i++) {
                if (edge_info[i].des != UINT32_MAX) {
                    neighbours[new_index++] = {edge_info[i].des, property[i]};
                }
            }


        } else {
            //type = 4;
            for (int i = 0; i < (2 << type); i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back({local_head_block_ptr->adj_vertex[i].des, local_head_block_ptr->adj_vertex[i].weight});
                }
            }
        }
    } else {
        // check edge array
        if (!local_head_block_ptr->next_block.load()) {
            ptr_block->buffer_locks[index_in_64]--;
            return false;
        }
        else {
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            block_size = edge_array[0];
            delete_space = edge_array[1];
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(neighbours.size() + block_size - delete_space);
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);

            for (int i = 0; i < block_size; i++) {
                if (edge_info[i].des != UINT32_MAX) {
                    neighbours[new_index++] = {edge_info[i].des, property[i]};
                }
            }
        }
    }
    ptr_block->buffer_locks[index_in_64]--;
    return true;
}

uint32_t SpruceTransVer::GetDegree(SpruceTransVer::TopBlock* root, const uint32_t from_node_id) {
    uint32_t degree = 0;
    //do not use locks
    uint32_t restart_num = 0;
    restart:
    uint64_t temp_ptr;
    auto from_node_id_low = (uint16_t) from_node_id;
    auto from_node_id_high = (uint16_t) (from_node_id >> 16);
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        return degree;
    }
    temp_ptr = root->ptr_to_children[from_node_id_high].load();
    auto* middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp_ptr;
    if (!middle_block_ptr) {
        return degree;
    }
    if (!get_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t) from_node_id_low)) {
        return degree;
    }
    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t) (from_node_id_low % 64);

    SpruceTransVer::AdjSubsequentBlockFive* bottom_head_block;


    SpruceTransVer::PtrBlock* ptr_block = (SpruceTransVer::PtrBlock*)middle_block_ptr->ptr_to_children[ptr_block_index].load();
    bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());


    if (!bottom_head_block) {
        return degree;
    }

    SpruceTransVer::AdjSubsequentBlockFive* local_head_block_ptr = bottom_head_block;
    uint32_t get_index = 0;
    uint64_t temp_bitvector;

//    recheck_bottom_lock:
//    while (local_head_block_ptr->mtx.load() != UNLOCKED);
//    uint32_t timestamp_before = bottom_head_block->timestamp.load();
//    // also recheck to ensure no write in this schedule gap
//    if (local_head_block_ptr->mtx.load() == WRITE_LOCKED) {
//        goto recheck_bottom_lock;
//    }

    degree += __builtin_popcountl(~(local_head_block_ptr->bitvector_64));
    auto type = local_head_block_ptr->type.load();
    if(type == 5) {
        temp_ptr = local_head_block_ptr->next_block.load();
        auto block_traverse_ptr = (SpruceTransVer::AdjSubsequentBlockFive*)temp_ptr;
        if (!block_traverse_ptr) {
            // only happens when block was deleted
            return degree;
        }
        else {
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_num;
            if (!edge_array) {
                block_size = 0;
                delete_num = 0;
            }
            else {
                block_size = edge_array[0];
                delete_num = edge_array[1];
            }
            degree = degree + block_size - delete_num;
        }
    }
    return degree;
}

bool SpruceTransVer::DeleteEdge(SpruceTransVer::TopBlock* root, const uint32_t from_node_id, const uint32_t to_node_id) {

    float temp_weight;      //for versions
    restart:
    uint64_t temp_ptr;
    auto from_node_id_low = (uint16_t) from_node_id;
    auto from_node_id_high = (uint16_t) (from_node_id >> 16);
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        return false;
    }
    temp_ptr = root->ptr_to_children[from_node_id_high].load();
    auto* middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp_ptr;
    if (!middle_block_ptr) {
        return false;
    }
    if (!get_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t)from_node_id_low)) {
        return false;
    }

    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t) (from_node_id_low % 64);

    SpruceTransVer::AdjSubsequentBlockFive* bottom_head_block;

    SpruceTransVer::PtrBlock* ptr_block = (SpruceTransVer::PtrBlock*)middle_block_ptr->ptr_to_children[ptr_block_index].load();
    bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());

    if (!bottom_head_block) {
        return false;
    }
    uint8_t unlocked_m = UNLOCKED, write_locked_m = WRITE_LOCKED;

    uint32_t get_index = 0;
    uint64_t temp_bitvector;
    uint32_t insert_index;
    uint8_t type;

    while(!ptr_block->buffer_locks[index_in_64].compare_exchange_strong(unlocked_m, write_locked_m)) {
        unlocked_m = UNLOCKED;
    }

    auto local_head_block_ptr = (SpruceTransVer::AdjSubsequentBlockFive*)ptr_block->ptr_to_buffer[index_in_64].load();

    if (!local_head_block_ptr) {
        ptr_block->buffer_locks[index_in_64]--;
        goto restart;
    }
    if (local_head_block_ptr->obsolete_flag) {
        ptr_block->buffer_locks[index_in_64]--;
        goto restart;
    }

    auto pre_bv = bottom_head_block->bitvector_64.load();

    local_head_block_ptr->timestamp == SpruceTransVer::get_global_timestamp();

    bool bottom_empty_flag = 0;
    if (local_head_block_ptr->bitvector_64 != UINT64_MAX) {
        //value existed
        uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
        auto type = local_head_block_ptr->type.load();
//        auto type_check_5 = reinterpret_cast<uint32_t*>(&local_head_block_ptr->bitvector_64);
//        auto type_check_4 = reinterpret_cast<uint16_t*>(&local_head_block_ptr->bitvector_64);
//        auto type_check_3 = reinterpret_cast<uint8_t*>(&local_head_block_ptr->bitvector_64);
        if (type == 5) {
            //type = 5;
            //firstly check the subsequent block
            for (int i = 0; i < 64; i++) {
                if (!get_bit(&pre_bv, i)) {
                    if (local_head_block_ptr->adj_vertex[i].des == to_node_id) {
//                        local_head_block_ptr->adj_vertex[i].des = UINT32_MAX;    // invalid
                        set_bit(&bottom_head_block->bitvector_64, i);
                        if (bottom_head_block->bitvector_64.load() == UINT64_MAX) {
                            temp_ptr = local_head_block_ptr->next_block.load();
                            if (!temp_ptr) {
                                //bottom_head_block empty
                                bottom_empty_flag = 1;
                                local_head_block_ptr->obsolete_flag.store(1);
                            }
                            else {
                                auto next_block_ptr = (uint32_t*)temp_ptr;
                                if (next_block_ptr[0] == next_block_ptr[1]) {
                                    //delete_num == edge array size
                                    local_head_block_ptr->next_block.store(0);
                                    bottom_empty_flag = 1;
                                    local_head_block_ptr->obsolete_flag.store(1);
                                    // add block_traverse_ptr and edge array ptr to delete queue

                                }
                            }
                        }
                        //may need to free edge array?

                        // add block_traverse_ptr and edge array ptr to delete queue
//                            free(block_traverse_ptr);
                        //edit head_block, slightly increase space(extra)
                        // TODO: edit after. using type or more checks
//                        clear_bit(&bottom_head_block->bitvector_64, 63);
                        temp_weight =local_head_block_ptr->adj_vertex[i].weight;
                        goto bottom_check;
                    }


                }
            }

            //check edge array
            temp_ptr = local_head_block_ptr->next_block.load();
            uint32_t* edge_array;
            if (!temp_ptr) {
                ptr_block->buffer_locks[index_in_64]--;
                return false;
            }
            else {
                edge_array = (uint32_t*)temp_ptr;
            }
            auto edge_array_size = edge_array[0];
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + edge_array_size);

            int32_t low = 0;
            int64_t high = edge_array_size - 1;
            int64_t mid;
            while (high >= low) {
                mid = (low + high) / 2;
                if ( edge_info[mid].des == UINT32_MAX) {
                    // find actual value
                    uint32_t  new_mid = mid;
                    while (new_mid < high) {
                        new_mid++;
                        if (edge_info[new_mid].des != UINT32_MAX) {
                            mid = new_mid;
                            break;
                        }
                    }
                }
                if(to_node_id < edge_info[mid].des){
                    high = mid - 1;
                }
                else if (to_node_id == edge_info[mid].des) {
                    edge_info[mid].des = UINT32_MAX;    // invalid
                    edge_array[1] += 1; //delete_num += 1
//                        block_traverse_ptr -> next_block_size -= 0;
                    if (edge_array[0] == edge_array[1]) {
                        local_head_block_ptr->next_block.store(0);
                        //add edge array pointer to the delete queue
//                        free(edge_array);
                    }
                    temp_weight =property[mid];
                    goto bottom_check;
                }
                else {
                    low = mid + 1;
                }
            }
            ptr_block->buffer_locks[index_in_64]--;
            return false;
        } else  {
            //type = 4;
            for (int i = 0; i < (2 << type); i++) {
                if (!get_bit(&pre_bv, i)) {
                    if (local_head_block_ptr->adj_vertex[i].des == to_node_id) {
//                        local_head_block_ptr->adj_vertex[i].des = UINT32_MAX;    // invalid
                        set_bit(&bottom_head_block->bitvector_64, i);
                        if (local_head_block_ptr->bitvector_64.load() == UINT64_MAX) {
                            //bottom_head_block empty
                            bottom_empty_flag = 1;
                            local_head_block_ptr->obsolete_flag.store(1);
                            //add to delete queue
//                            free(block_traverse_ptr);
                        }
                        temp_weight =local_head_block_ptr->adj_vertex[i].weight;
                        goto bottom_check;
                    }
                }
            }
            ptr_block->buffer_locks[index_in_64]--;
            return false;
        }
    } else {
        if (local_head_block_ptr->next_block.load()) {
            //only check edge array (subsequent block is empty)
            temp_ptr = local_head_block_ptr->next_block.load();
            uint32_t* edge_array;
            edge_array = (uint32_t*)temp_ptr;

            auto edge_array_size = edge_array[0];
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + edge_array_size);

            int64_t low = 0;
            int64_t high = edge_array_size - 1;
            int64_t mid;
            while (high >= low) {
                mid = (low + high) / 2;
                if ( edge_info[mid].des == UINT32_MAX) {
                    // find actual value
                    uint32_t  new_mid = mid;
                    while (new_mid < high) {
                        new_mid++;
                        if (edge_info[new_mid].des != UINT32_MAX) {
                            mid = new_mid;
                            break;
                        }
                    }
                }
                if(to_node_id < edge_info[mid].des){
                    high = mid - 1;
                }
                else if (to_node_id == edge_info[mid].des) {
                    edge_info[mid].des = UINT32_MAX;    // invalid
                    edge_array[1] += 1; //delete_num += 1
//                        block_traverse_ptr -> next_block_size -= 0;
                    if (edge_array[0] == edge_array[1]) {
                        local_head_block_ptr->next_block.store(0);
                        //add edge array pointer to the delete queue
//                        free(edge_array);
                    }
                    temp_weight = property[mid];
                    goto bottom_check;
                }
                else {
                    low = mid + 1;
                }
            }
            ptr_block->buffer_locks[index_in_64]--;
            return false;
        }
        else {
            ptr_block->buffer_locks[index_in_64]--;
            return false;
        }
    }


    bottom_check:
    //first_try_to_add_versions
    SpruceTransVer::AddVersion(DELETE_TYPE, local_head_block_ptr, {to_node_id, static_cast<uint32_t>(local_head_block_ptr->timestamp),temp_weight});

    //edit blocks and bits from bottom to top
    ptr_block->buffer_locks[index_in_64]--;
//    bottom_empty_flag = 0;
    if (bottom_empty_flag == 1) {
        //get lock and edit bitmap
        while(!middle_block_ptr->mtx[ptr_block_index].compare_exchange_strong(unlocked_m, write_locked_m)) {
            unlocked_m = UNLOCKED;
        }
        clear_bit(&middle_block_ptr->bitmap_8kb, (uint32_t) from_node_id_low);
        middle_block_ptr->mtx[ptr_block_index].store(UNLOCKED);
        if (ptr_block) {
            //corresponding ptr block exists
            ptr_block->ptr_to_buffer[index_in_64].store(0);
        }
        //Check if bitmap == 0;
//        middle_block_ptr->mtx[ptr_block_index].store(UNLOCKED);
        bool bv_empty_flag = 1;

        for (unsigned char &i: middle_block_ptr->bitmap_8kb) {
            if (i != 0) {
                bv_empty_flag = 0;
                break;
            }
        }
        if (bv_empty_flag) {
            // lock all middle block locks and recheck
            for (int l = 0; l < 1024; l++) {
                while(!middle_block_ptr->mtx[l].compare_exchange_strong(unlocked_m, write_locked_m)) {
                    unlocked_m = UNLOCKED;
                }
            }
            // recheck
            bv_empty_flag = 1;
            for (unsigned char &i: middle_block_ptr->bitmap_8kb) {
                if (i != 0) {
                    bv_empty_flag = 0;
                    break;
                }
            }
            if (bv_empty_flag) {
                // add middle block to delete queue
                middle_block_ptr->obsolete_flag.store(1);
            }
            for (int l = 0; l < 1024; l++) {
                middle_block_ptr->mtx[l].store(UNLOCKED);
            }
        }

        if (bv_empty_flag) {
            // lock top block and edit bitmap
            while(!root->mtx[from_node_id_high/64].compare_exchange_strong(unlocked_m, write_locked_m)) {
                unlocked_m = UNLOCKED;
            }
            //add m-b to delete queue
//            free(middle_block_ptr);
            clear_bit(&root->bitmap_8kb, from_node_id_high);
            root->ptr_to_children[from_node_id_high].store(0);
            root->mtx[from_node_id_high/64].store(UNLOCKED);
        }
//        free(bottom_head_block);
    } else {
        return true;
    }
    return true;
}

bool SpruceTransVer::get_neighbours_sorted(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
                                       std::vector<WeightedOutEdgeSimple> &neighbours) {

    uint32_t restart_num = 0;
    restart:
    neighbours.clear();
    uint64_t temp_ptr;
    auto from_node_id_low = (uint16_t) from_node_id;
    auto from_node_id_high = (uint16_t) (from_node_id >> 16);
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        return false;
    }
    temp_ptr = root->ptr_to_children[from_node_id_high].load();
    auto* middle_block_ptr = (Spruce::MiddleBlock*)temp_ptr;
    if (!middle_block_ptr) {
        return false;
    }
    if (!get_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t) from_node_id_low)) {
        return false;
    }
    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t) (from_node_id_low % 64);

    SpruceTransVer::AdjSubsequentBlockFive* bottom_head_block;

    SpruceTransVer::PtrBlock* ptr_block = (SpruceTransVer::PtrBlock*)middle_block_ptr->ptr_to_children[ptr_block_index].load();
    bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());

    if (!bottom_head_block) {
        return false;
    }

//    Spruce::AdjSubsequentBlock* local_subsequent_block_ptr;
    SpruceTransVer::AdjSubsequentBlockFive* local_head_block_ptr = bottom_head_block;
    uint32_t get_index = 0;
    uint64_t temp_bitvector;

    recheck_bottom_lock:
    while (ptr_block->buffer_locks[index_in_64].load() != UNLOCKED);
    uint32_t timestamp_before = bottom_head_block->timestamp.load();
    // also recheck
    if (ptr_block->buffer_locks[index_in_64].load() == WRITE_LOCKED) {
        goto recheck_bottom_lock;
    }


    if (local_head_block_ptr->bitvector_64 != UINT64_MAX) {
        //value existed
        uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
//        auto type_check_5 = reinterpret_cast<uint32_t*>(&local_head_block_ptr->bitvector_64);
//        auto type_check_4 = reinterpret_cast<uint16_t*>(&local_head_block_ptr->bitvector_64);
//        auto type_check_3 = reinterpret_cast<uint8_t*>(&local_head_block_ptr->bitvector_64);
        auto pre_bv = bottom_head_block->bitvector_64.load();
        auto type = local_head_block_ptr->type.load();
        if (type == 5) {
            //type = 5;
            //first check subsequent block
            std::vector<SpruceTransVer::WeightedOutEdgeSimple> temp_neighbours;
            for (int i = 0; i < 64; i++) {
                if (!get_bit(&pre_bv, i)) {
                    temp_neighbours.push_back({local_head_block_ptr->adj_vertex[i].des, local_head_block_ptr->adj_vertex[i].weight});
                }
            }
            sort(temp_neighbours.begin(), temp_neighbours.end(), CompareOutEdges);
            //then check edge array
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            if (!edge_array) {
                block_size = 0;
                delete_space = 0;
            }
            else {
                block_size = edge_array[0];
                delete_space = edge_array[1];
            }
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(temp_neighbours.size() + block_size -delete_space);

            uint32_t start1 = 0, start2 = 0;
            uint32_t end1 = temp_neighbours.size(), end2 = block_size;
            uint32_t  k = 0;
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);
//            SpruceTransVer::WeightedOutEdgeSimple a1,a2;

            while (start1 < end1 && start2 < end2) {
                if (edge_info[start2].des == UINT32_MAX) {
                    // skip invalid data
                    start2++;
                    continue;
                }
                if (temp_neighbours[start1].des < edge_info[start2].des) {
                    neighbours[k++] = {temp_neighbours[start1].des, temp_neighbours[start1].weight};
                    start1++;
                }
                else {
                    neighbours[k++] = {edge_info[start2].des, property[start2]};
                    start2++;
                }
            }
            while (start1 < end1) {
                neighbours[k++] = {temp_neighbours[start1++].des, temp_neighbours[start1++].weight};
            }
            while (start2 < end2) {
                if (edge_info[start2].des == UINT32_MAX) {
                    // skip invalid data
                    start2++;
                    continue;
                }
                neighbours[k++] = {edge_info[start2++].des, property[start2]};
                start2++;
            }


        } else {
            //type = 4;
            for (int i = 0; i < (2 << type); i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back({local_head_block_ptr->adj_vertex[i].des, local_head_block_ptr->adj_vertex[i].weight});
                }
            }
            sort(neighbours.begin(), neighbours.end(), CompareOutEdges);
        }
    } else {
        // just check edge array
        temp_ptr = local_head_block_ptr->next_block.load();
        auto edge_array = (uint32_t*)temp_ptr;
        uint32_t block_size, delete_space;
        if (!edge_array) {
            return false;
        }
        else {
            block_size = edge_array[0];
            delete_space = edge_array[1];
        }
        uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
        neighbours.resize(block_size -delete_space);

        uint32_t start2 = 0;
        uint32_t end2 = block_size;
        uint32_t  k = 0;
        auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
        auto edge_info = (DesT*)(edges_ptr);
        auto property = (double*)(edge_info + block_size);
        while (start2 < end2) {
            if (edge_info[start2].des == UINT32_MAX) {
                // skip invalid data
                start2++;
                continue;
            }
            neighbours[k++] = {edge_info[start2].des, property[start2]};
            start2++;
        }
    }
    uint32_t timestamp_after = bottom_head_block->timestamp.load();
    if (timestamp_before != timestamp_after) {
        if (restart_num < RESTART_THRESHOLD){
            restart_num++;
            goto restart;
        }
    }
    return true;
}

bool SpruceTransVer::get_neighbours_sorted_only(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
                                           std::vector<uint32_t> &neighbours) {

    uint32_t restart_num = 0;
    restart:
    neighbours.clear();
    uint64_t temp_ptr;
    auto from_node_id_low = (uint16_t) from_node_id;
    auto from_node_id_high = (uint16_t) (from_node_id >> 16);
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        return false;
    }
    temp_ptr = root->ptr_to_children[from_node_id_high].load();
    auto* middle_block_ptr = (Spruce::MiddleBlock*)temp_ptr;
    if (!middle_block_ptr) {
        return false;
    }
    if (!get_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t) from_node_id_low)) {
        return false;
    }
    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t) (from_node_id_low % 64);

    SpruceTransVer::AdjSubsequentBlockFive* bottom_head_block;

    SpruceTransVer::PtrBlock* ptr_block = (SpruceTransVer::PtrBlock*)middle_block_ptr->ptr_to_children[ptr_block_index].load();
    bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());

    if (!bottom_head_block) {
        return false;
    }

//    Spruce::AdjSubsequentBlock* local_subsequent_block_ptr;
    SpruceTransVer::AdjSubsequentBlockFive* local_head_block_ptr = bottom_head_block;
    uint32_t get_index = 0;
    uint64_t temp_bitvector;

    recheck_bottom_lock:
    while (ptr_block->buffer_locks[index_in_64].load() != UNLOCKED);
    uint32_t timestamp_before = bottom_head_block->timestamp.load();
    // also recheck
    if (ptr_block->buffer_locks[index_in_64].load() == WRITE_LOCKED) {
        goto recheck_bottom_lock;
    }


    if (local_head_block_ptr->bitvector_64 != UINT64_MAX) {
        //value existed
        uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
//        auto type_check_5 = reinterpret_cast<uint32_t*>(&local_head_block_ptr->bitvector_64);
//        auto type_check_4 = reinterpret_cast<uint16_t*>(&local_head_block_ptr->bitvector_64);
//        auto type_check_3 = reinterpret_cast<uint8_t*>(&local_head_block_ptr->bitvector_64);
        auto pre_bv = bottom_head_block->bitvector_64.load();
        auto type = local_head_block_ptr->type.load();
        if (type == 5) {
            //type = 5;
            //first check subsequent block
            std::vector<uint32_t> temp_neighbours;
            for (int i = 0; i < 64; i++) {
                if (!get_bit(&pre_bv, i)) {
                    temp_neighbours.push_back(local_head_block_ptr->adj_vertex[i].des);
                }
            }
            sort(temp_neighbours.begin(), temp_neighbours.end());
            //then check edge array
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            if (!edge_array) {
                block_size = 0;
                delete_space = 0;
            }
            else {
                block_size = edge_array[0];
                delete_space = edge_array[1];
            }
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(temp_neighbours.size() + block_size -delete_space);

            uint32_t start1 = 0, start2 = 0;
            uint32_t end1 = temp_neighbours.size(), end2 = block_size;
            uint32_t  k = 0;
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);
//            SpruceTransVer::WeightedOutEdgeSimple a1,a2;

            while (start1 < end1 && start2 < end2) {
                if (edge_info[start2].des == UINT32_MAX) {
                    // skip invalid data
                    start2++;
                    continue;
                }
                if (temp_neighbours[start1] < edge_info[start2].des) {
                    neighbours[k++] = temp_neighbours[start1];
                    start1++;
                }
                else {
                    neighbours[k++] = edge_info[start2].des;
                    start2++;
                }
            }
            while (start1 < end1) {
                neighbours[k++] = temp_neighbours[start1++];
            }
            while (start2 < end2) {
                if (edge_info[start2].des == UINT32_MAX) {
                    // skip invalid data
                    start2++;
                    continue;
                }
                neighbours[k++] = edge_info[start2++].des;
                start2++;
            }


        } else {
            //type = 4;
            for (int i = 0; i < (2 << type); i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back(local_head_block_ptr->adj_vertex[i].des);
                }
            }
            sort(neighbours.begin(), neighbours.end());
        }
    } else {
        // just check edge array
        temp_ptr = local_head_block_ptr->next_block.load();
        auto edge_array = (uint32_t*)temp_ptr;
        uint32_t block_size, delete_space;
        if (!edge_array) {
            return false;
        }
        else {
            block_size = edge_array[0];
            delete_space = edge_array[1];
        }
        uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
        neighbours.resize(block_size -delete_space);

        uint32_t start2 = 0;
        uint32_t end2 = block_size;
        uint32_t  k = 0;
        auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
        auto edge_info = (DesT*)(edges_ptr);
        auto property = (double*)(edge_info + block_size);
        while (start2 < end2) {
            if (edge_info[start2].des == UINT32_MAX) {
                // skip invalid data
                start2++;
                continue;
            }
            neighbours[k++] = edge_info[start2].des;
            start2++;
        }
    }
    uint32_t timestamp_after = bottom_head_block->timestamp.load();
    if (timestamp_before != timestamp_after) {
        if (restart_num < RESTART_THRESHOLD){
            restart_num++;
            goto restart;
        }
    }
    return true;
}

bool SpruceTransVer::UpdateEdge(SpruceTransVer::TopBlock* root, SpruceTransVer::WeightedEdge edge) {
    if (edge.weight < 0) {
        SpruceTransVer::DeleteEdge(root, edge.src, edge.des);
    }
    auto from_node_id = edge.src;
    auto to_node_id = edge.des;
    auto weight = edge.weight;
    //set up lock status
    int unlocked = UNLOCKED, read_locked = READ_LOCKED, write_locked = WRITE_LOCKED;
    uint8_t unlocked_m = UNLOCKED;
    uint8_t write_locked_m = WRITE_LOCKED;
    auto from_node_id_low = (uint16_t)from_node_id;
    auto from_node_id_high = (uint16_t)(from_node_id >> 16);
    //need lock: in case for deletion
    SpruceTransVer::MiddleBlock* middle_block_ptr;
//    root->mtx.lock_upgrade();
    restart:
    //
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        //Noted that when thread get lock, corresponding middle block may be created, thereby recheck is needed;
        //try to get write lock (spin lock)
        while(!root->mtx[from_node_id_high/64].compare_exchange_strong(unlocked_m, write_locked_m)) {
            unlocked_m = UNLOCKED;
        }
//        root->mtx.unlock_upgrade_and_lock();
        if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
            // Need to malloc new middle block
            middle_block_ptr = (SpruceTransVer::MiddleBlock*) malloc(sizeof(SpruceTransVer::MiddleBlock));
            memset(middle_block_ptr, 0, sizeof(SpruceTransVer::MiddleBlock));
            root->ptr_to_children[from_node_id_high].store((uint64_t)middle_block_ptr);
            set_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high);
            //Analysis
            middle_block_num_++;
        }
        else {
            //atomically load
            uint64_t temp = root->ptr_to_children[from_node_id_high].load();
            middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp;
        }
        root->mtx[from_node_id_high/64].store(UNLOCKED);
    }
    else {
        //read atomically
        uint64_t temp = root->ptr_to_children[from_node_id_high].load();
        middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp;
//        root->mtx.unlock_upgrade();
    }

    // in case for deletion
    if (!middle_block_ptr) {
        goto restart;
    }
    if (middle_block_ptr->obsolete_flag) {
        goto restart;
    }

    //now we need to get corresponding uint64_t and check the number of 1
    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
//    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t)(from_node_id_low % 64);

    //Decide the ptr type due to the number of 1

    SpruceTransVer::AdjSubsequentBlockOne* bottom_head_block;

    int lock_flag = 0;
    unlocked = UNLOCKED;

    if (!get_bit(&auxiliary_64, index_in_64)) {
        // does not exist, lock;
        while(!middle_block_ptr->mtx[ptr_block_index].compare_exchange_strong(unlocked_m, write_locked_m)) {
            unlocked_m = UNLOCKED;
        }
        lock_flag = 1;
    }




    //Get bottom head block
    // change set bit sequence for parallel: firstly edit pointer, then set bit
    // reget values!!!!
    auxiliary_64 = auxiliary_ptr[ptr_block_index];
    auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    SpruceTransVer::PtrBlock* ptr_block;
    // recheck
    if (!get_bit(&auxiliary_64, index_in_64)) {
        //bottom block does not exist, malloc a new block
        bottom_head_block = (SpruceTransVer::AdjSubsequentBlockOne*) malloc(sizeof(SpruceTransVer::AdjSubsequentBlockOne));
        memset(bottom_head_block, 0, sizeof(SpruceTransVer::AdjSubsequentBlockOne));
        bottom_head_block->bitvector_64 = UINT64_MAX;
        bottom_head_block->type.store(1);

        //Edit middle block bitmap and ptr block
        uint64_t temp = middle_block_ptr->ptr_to_children[ptr_block_index].load();
        ptr_block = (SpruceTransVer::PtrBlock*)temp;

        if(!ptr_block) {
            // + 1 for obsolete flag
            auto new_ptr_block = (SpruceTransVer::PtrBlock*) malloc(sizeof(SpruceTransVer::PtrBlock)) ;
            memset(new_ptr_block, 0, sizeof(SpruceTransVer::PtrBlock));
            new_ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<unsigned long>(bottom_head_block));
            middle_block_ptr->ptr_to_children[ptr_block_index].store((uint64_t)new_ptr_block);
            ptr_block = new_ptr_block;
            sparse_ptr_block_num++;
        }
        else {
            if (ptr_block->obsolete_flag) {
                //obsoleted
                if (lock_flag) {
                    middle_block_ptr->mtx[ptr_block_index]--;
                }
                goto restart;
            }
            ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<unsigned long>(bottom_head_block));
        }


        set_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t) from_node_id_low);
        //Analysis
//        adj_head_num_++;
        type1++;
    } else {
        //corresponding block exists
        uint64_t temp = middle_block_ptr->ptr_to_children[ptr_block_index].load();
        ptr_block = (SpruceTransVer::PtrBlock*)temp;
//        bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());
    }



    unlocked = UNLOCKED;
    if (lock_flag) {
        middle_block_ptr->mtx[ptr_block_index]--;
    }


    uint32_t insert_index;
    uint8_t type;
    uint64_t temp_ptr;

    while(!ptr_block->buffer_locks[index_in_64].compare_exchange_strong(unlocked_m, write_locked_m)) {
        unlocked_m = UNLOCKED;
    }
    auto local_head_block_ptr = (SpruceTransVer::AdjSubsequentBlockFive*)ptr_block->ptr_to_buffer[index_in_64].load();
    if (!local_head_block_ptr) {
        ptr_block->buffer_locks[index_in_64]--;
        goto restart;
    }
    if (local_head_block_ptr->obsolete_flag) {
        ptr_block->buffer_locks[index_in_64]--;
        goto restart;
    }

    auto pre_bv = local_head_block_ptr->bitvector_64.load();

    //Edit timestamp
    local_head_block_ptr->timestamp = SpruceTransVer::get_global_timestamp();

    //firstly check whether edge exists
    //value existed
    uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
    type = local_head_block_ptr->type.load();
    if (type == 5) {
        //type = 5;
        //firstly check the subsequent block
        for (int i = 0; i < 64; i++) {
            if (!get_bit(&pre_bv, i)) {
                if (local_head_block_ptr->adj_vertex[i].des == to_node_id) {
                    AddVersion(UPDATE_TYPE, local_head_block_ptr, {to_node_id, static_cast<uint32_t>(local_head_block_ptr->timestamp), local_head_block_ptr->adj_vertex[i].weight});
                    local_head_block_ptr->adj_vertex[i].weight = weight;
                    local_head_block_ptr->adj_vertex[i].timestamp = local_head_block_ptr->timestamp;
                    ptr_block->buffer_locks[index_in_64]--;
                    return true;
                }
            }
        }

        //check edge array
        temp_ptr = local_head_block_ptr->next_block.load();
        uint32_t* edge_array;
        if (!temp_ptr) {
            goto insert_update;
        }
        else {
            edge_array = (uint32_t*)temp_ptr;
        }
        auto edge_array_size = edge_array[0];
        auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
        auto edge_info = (DesT*)(edges_ptr);
        auto property = (double*)(edge_info + edge_array_size);

        int64_t low = 0;
        int64_t high = edge_array_size - 1;
        int64_t mid;
        while (high >= low) {
            mid = (low + high) / 2;
            if ( edge_info[mid].des == UINT32_MAX) {
                // find actual value
                uint32_t  new_mid = mid;
                while (new_mid < high) {
                    new_mid++;
                    if (edge_info[new_mid].des != UINT32_MAX) {
                        mid = new_mid;
                        break;
                    }
                }
            }
            if(to_node_id < edge_info[mid].des){
                high = mid - 1;
            }
            else if (to_node_id == edge_info[mid].des) {
                AddVersion(UPDATE_TYPE, local_head_block_ptr, {to_node_id, static_cast<uint32_t>(local_head_block_ptr->timestamp),property[mid]});
                property[mid] = weight;
                edge_info[mid].timestamp = local_head_block_ptr->timestamp;
                ptr_block->buffer_locks[index_in_64]--;
                return true;
            }
            else {
                low = mid + 1;
            }
        }
        goto insert_update;
    } else {
        for (int i = 0; i < (2 << type); i++) {
            if (!get_bit(&pre_bv, i)) {
                if (local_head_block_ptr->adj_vertex[i].des == to_node_id) {
                    AddVersion(UPDATE_TYPE, local_head_block_ptr, {to_node_id, static_cast<uint32_t>(local_head_block_ptr->timestamp),local_head_block_ptr->adj_vertex[i].weight});
                    local_head_block_ptr->adj_vertex[i].weight = weight;
                    local_head_block_ptr->adj_vertex[i].timestamp = local_head_block_ptr->timestamp;
                    ptr_block->buffer_locks[index_in_64]--;
                    return true;                    }
            }
        };
        goto insert_update;
    }



    //head insertion
    insert_update:
    WeightedOutEdge out_edge = {edge.des, static_cast<uint32_t>(local_head_block_ptr->timestamp),edge.weight};
    if (local_head_block_ptr->bitvector_64 != 0) {
        //not full yet
        uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
        insert_index = __builtin_clzl(temp_bv_rev);
        type = local_head_block_ptr->type;
        clear_bit(&local_head_block_ptr->bitvector_64, insert_index);
        uint64_t temp;
        if (insert_index > ((1 << (type - 1)) * 4 - 1)) {
            //need to malloc new space;
            switch (type) {
                case 1: {
                    auto new_block = (SpruceTransVer::AdjSubsequentBlockTwo*) malloc(
                            sizeof(AdjSubsequentBlockTwo));
                    memset(new_block, 0, sizeof(AdjSubsequentBlockTwo));
                    temp = (uint64_t)local_head_block_ptr;
                    memcpy(new_block,
                           (SpruceTransVer::AdjSubsequentBlockOne*)(temp),
                           sizeof(SpruceTransVer::AdjSubsequentBlockOne));
                    new_block->adj_vertex[insert_index] = out_edge;
                    new_block->type.store(2);
                    local_head_block_ptr->obsolete_flag = 1;
                    // add previous block to delete queue
                    ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<uint64_t>(new_block));
//                        free((SpruceTransVer::AdjSubsequentBlockOne*)(temp));
                    local_head_block_ptr = (SpruceTransVer::AdjSubsequentBlockFive*)new_block;
                    //analysis
                    type2++;
                    type1--;

                    break;
                }
                case 2: {
                    auto new_block = (SpruceTransVer::AdjSubsequentBlockThree*) malloc(
                            sizeof(AdjSubsequentBlockThree));
                    memset(new_block, 0, sizeof(AdjSubsequentBlockThree));
                    temp = (uint64_t)local_head_block_ptr;
                    memcpy(new_block,
                           (SpruceTransVer::AdjSubsequentBlockTwo*)temp,
                           sizeof(SpruceTransVer::AdjSubsequentBlockTwo));
                    new_block->adj_vertex[insert_index] = out_edge;
                    new_block->type.store(3);
                    local_head_block_ptr->obsolete_flag = 1;
                    // add previous block to delete queue
                    ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<uint64_t>(new_block));
//                        free((SpruceTransVer::AdjSubsequentBlockTwo*)temp);
                    //analysis
                    type3++;
                    type2--;

                    break;
                }
                case 3: {
                    auto new_block = (SpruceTransVer::AdjSubsequentBlockFour*) malloc(
                            sizeof(AdjSubsequentBlockFour));
                    memset(new_block, 0, sizeof(AdjSubsequentBlockFour));
                    temp = (uint64_t)local_head_block_ptr;
                    memcpy(new_block,
                           (SpruceTransVer::AdjSubsequentBlockThree*)temp,
                           sizeof(SpruceTransVer::AdjSubsequentBlockThree));
                    new_block->adj_vertex[insert_index] = out_edge;
                    new_block->type.store(4);
                    local_head_block_ptr->obsolete_flag = 1;
                    // add previous block to delete queue
                    ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<uint64_t>(new_block));
//                        free((SpruceTransVer::AdjSubsequentBlockThree*)temp);
                    //analysis
                    type4++;
                    type3--;

                    break;

                }
                case 4: {
                    auto new_block = (SpruceTransVer::AdjSubsequentBlockFive*) malloc(
                            sizeof(AdjSubsequentBlockFive));
                    temp = (uint64_t)local_head_block_ptr;
                    memset(new_block, 0, sizeof(AdjSubsequentBlockFive));
                    // new_block->bitvector_64 = UINT64_MAX; //no subsequent block
                    memcpy(new_block,
                           (SpruceTransVer::AdjSubsequentBlockFour*)(temp),
                           sizeof(SpruceTransVer::AdjSubsequentBlockFour));
                    new_block->adj_vertex[insert_index] = out_edge;
                    new_block->type.store(5);
                    local_head_block_ptr->obsolete_flag = 1;
                    // add previous block to delete queue
                    ptr_block->ptr_to_buffer[index_in_64].store(reinterpret_cast<uint64_t>(new_block));
//                        free((SpruceTransVer::AdjSubsequentBlockFour*)temp);
                    //analysis
                    type5++;
                    type4--;

                    break;
                }
                default: {
                    std::cout << "Impossible type!" << std::endl;
                    break;
                }

            }
        } else {
            local_head_block_ptr->adj_vertex[insert_index] = out_edge;
        }
    } else {
        type = 5;
        // while ends, no spaces
        // Firstly check the block size,we assume that we only check invalid label in deletion
        uint32_t old_block_size, old_delete_num;
        uint64_t temp1 = (uint64_t)local_head_block_ptr;
        uint64_t temp2 = local_head_block_ptr->next_block.load();
        auto old_edge_array = (uint32_t*)temp2;

//        EdgeTree* edge_tree;
        // edge tree version
//        if (!old_edge_array) {
//            edge_tree = new EdgeTree();
//            local_head_block_ptr->next_block.store((uint64_t)edge_tree);
//        }
//        else {
//            edge_tree = (EdgeTree*)temp2;
//        }
//
//        for (int i = 0; i < 64; i++) {
//            edge_tree->insert(local_head_block_ptr->adj_vertex[i].des, {double(local_head_block_ptr->adj_vertex[i].weight), local_head_block_ptr->adj_vertex[i].timestamp});
//        }


        if (!old_edge_array) {
            // does not exist, set initial size

            old_block_size = 0;
            old_delete_num = 0;
            // analysis
            edge_array_num++;
        }
        else {
            old_block_size = old_edge_array[0];
            old_delete_num = old_edge_array[1];
        }

        auto old_edges = (SpruceTransVer::WeightedOutEdge*)(old_edge_array + 2);

        int old_delete_64 = 0;
        if (old_delete_num > 64) {
            old_delete_64 = old_delete_num/64;
        }

        // + 1 for block_size and delete_num (equals to weighted out edge)
        // resize block according to delete_num
        auto new_block_size = (64 + 1 + old_block_size - old_delete_64 * 64);
        auto new_edge_array = (uint32_t*) malloc(
                sizeof(SpruceTransVer::WeightedOutEdge) * (new_block_size));
        memset(new_edge_array, 0, sizeof(SpruceTransVer::WeightedOutEdge) * (new_block_size));
//

        //sort the vertex using bubble sort
        for (int i = 0; i < 64; i++) {
            bool flag = false;
            for (int j = 0; j < 64 - i - 1; j++) {
                if (local_head_block_ptr->adj_vertex[j].des > local_head_block_ptr->adj_vertex[j + 1].des) {
                    auto temp = local_head_block_ptr->adj_vertex[j];
                    local_head_block_ptr->adj_vertex[j] = local_head_block_ptr->adj_vertex[j + 1];
                    local_head_block_ptr->adj_vertex[j + 1] = temp;
                    flag = true;
                }
            }
            if (!flag) {
                break;
            }
        }

        //then use merge sort to place spaces in new block
        uint32_t k = 0;
        uint32_t start1 = 0, start2 = 0;
        uint32_t end1 = 64, end2 = old_block_size;

        auto new_edges = (SpruceTransVer::WeightedOutEdge*)(new_edge_array + 2);
        auto new_edge_info = (DesT*)(new_edges);
        auto new_property = (double*)(new_edge_info + (old_block_size + 64 - old_delete_64 * 64));
        auto old_edge_info = (DesT*)(old_edges);
        auto old_property = (double*)(old_edge_info + old_block_size);

        while (start1 < end1 && start2 < end2) {
            if (old_edge_info[start2].des == UINT32_MAX) {
                // skip invalid data
                start2++;
                continue;
            }
            if (local_head_block_ptr->adj_vertex[start1].des < old_edge_info[start2].des) {
                new_edge_info[k] = {local_head_block_ptr->adj_vertex[start1].des, local_head_block_ptr->adj_vertex[start1].timestamp};
                new_property[k++] = local_head_block_ptr->adj_vertex[start1++].weight;
            }
            else {
                new_edge_info[k] = old_edge_info[start2];
                new_property[k++] = old_property[start2++];
            }
//            new_edges[k++] = local_head_block_ptr->adj_vertex[start1].des < old_edges[start2].des ?
//                             local_head_block_ptr->adj_vertex[start1++] : old_edges[start2++];
        }
        while (start1 < end1) {
            new_edge_info[k] = {local_head_block_ptr->adj_vertex[start1].des, local_head_block_ptr->adj_vertex[start1].timestamp};
            new_property[k++] = local_head_block_ptr->adj_vertex[start1++].weight;
        }
        while (start2 < end2) {
            if (old_edge_info[start2].des == UINT32_MAX) {
                // skip invalid data
                start2++;
                continue;
            }
            new_edge_info[k] = old_edge_info[start2];
            new_property[k++] = old_property[start2++];
        }
        while (k < new_block_size - 1) {
            // shift invalid values with size < 64
            new_edge_info[k++].des = UINT32_MAX;
        }

//        //debug
//        if (new_block_size > 8000 && edge.src == 17) {
//            for (int i = 0; i < new_block_size; i++) {
//                std::cout <<new_edge_info[i].des << std::endl;
//            }
//        }




        //reset subsequent block status
        memset(local_head_block_ptr->adj_vertex, 0, sizeof(SpruceTransVer::WeightedOutEdge)*64);
        new_edge_array[0] = new_block_size - 1; // remember: block_size does not include first 2 uint32_t for information
        new_edge_array[1] = old_delete_num % 64;  // copy delete_num
        local_head_block_ptr->next_block.store(reinterpret_cast<uint64_t>(new_edge_array));
        //do not execute free function in new;
        // Noted that when execute on large datasets, use free function to avoid memory exceeded
        // when execute parallel test, comment it to avoid errors
        if(old_edge_array) free(old_edge_array);


        //reset head block status
        local_head_block_ptr->bitvector_64.store(UINT64_MAX);

        //Then insert new edge to subsequent block
        local_head_block_ptr->adj_vertex[0] = out_edge;
        clear_bit(&local_head_block_ptr->bitvector_64, 0);

        //analysis
        type64++;

    }
    ptr_block->buffer_locks[index_in_64]--;
    return true;
}

bool SpruceTransVer::AddVersion(uint8_t type, SpruceTransVer::AdjSubsequentBlockFive* local_head_block_ptr,
                                SpruceTransVer::WeightedOutEdge edge) {
//        return true;
        auto inner_offset = local_head_block_ptr->log_size % BLOCK_SIZE;
        auto outer_offset = local_head_block_ptr->log_size / BLOCK_SIZE;
        auto log_block_ptr = (SpruceTransVer::LogBlock*)local_head_block_ptr->ptr_to_log_block.load();
        SpruceTransVer::LogBlock* temp_ptr;
        if (inner_offset == 0) {
            // need to malloc new blocks
            if (outer_offset == BLOCK_THRESHOLD) {
                // block full, just reuse the last block
                for (int i = 0; i < BLOCK_THRESHOLD - 1; i++) {
                    log_block_ptr = log_block_ptr->next_block;
                    if (i == BLOCK_THRESHOLD - 3) {
//                        //debug
//                        for (int j = 0; j < BLOCK_SIZE; j++) {
//                            std::cout << log_block_ptr->versioned_edges[j].timestamp << std::endl;
//                        }
                        temp_ptr = log_block_ptr;
                    }
                }
                temp_ptr->next_block = NULL;
                memset(log_block_ptr, 0, sizeof(SpruceTransVer::LogBlock));
                log_block_ptr->next_block = (SpruceTransVer::LogBlock*) local_head_block_ptr->ptr_to_log_block.load();
                local_head_block_ptr->log_size -= (BLOCK_SIZE-1); // -64 + 1
                log_block_ptr->versioned_edges[inner_offset] = {(uint64_t)type << 56 | local_head_block_ptr->timestamp,
                                                                  edge}; //delete, type 0
                local_head_block_ptr->ptr_to_log_block = (uint64_t)log_block_ptr;
            }
            else {
                // malloc new blocks
                auto log_block_ptr_new = (SpruceTransVer::LogBlock*)malloc(sizeof(SpruceTransVer::LogBlock));
                if (log_block_ptr_new == NULL) return false;
                memset(log_block_ptr_new, 0, sizeof(SpruceTransVer::LogBlock));
                log_block_ptr_new->next_block = log_block_ptr;
                log_block_ptr_new->versioned_edges[inner_offset] = {((uint64_t)type) << 56 | local_head_block_ptr->timestamp, edge};
                local_head_block_ptr->log_size++;
                //debug
//                std::cout << local_head_block_ptr->log_size << std::endl;
                local_head_block_ptr->ptr_to_log_block = (uint64_t)log_block_ptr_new;
            }
        }
        else {
            log_block_ptr->versioned_edges[inner_offset] = {((uint64_t)type) << 56 | local_head_block_ptr->timestamp, edge};
            local_head_block_ptr->log_size++;
        }
        return true;
}

uint64_t SpruceTransVer::get_global_timestamp() {
    return global_timestamp++;
}

bool SpruceTransVer::get_neighbours_snapshot(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
                                    std::vector<WeightedOutEdgeSimple> &neighbours) {
    auto read_timestamp = SpruceTransVer::get_global_timestamp();

    //do not use locks
    uint32_t restart_num = 0;
    restart:
    neighbours.clear();
    uint64_t temp_ptr;
    auto from_node_id_low = (uint16_t) from_node_id;
    auto from_node_id_high = (uint16_t) (from_node_id >> 16);
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        return false;
    }
    temp_ptr = root->ptr_to_children[from_node_id_high].load();
    auto* middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp_ptr;
    if (!middle_block_ptr) {
        return false;
    }
    if (!get_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t)from_node_id_low)) {
        return false;
    }
    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t) (from_node_id_low % 64);

    SpruceTransVer::AdjSubsequentBlockFive* bottom_head_block;

    SpruceTransVer::PtrBlock* ptr_block = (SpruceTransVer::PtrBlock*)middle_block_ptr->ptr_to_children[ptr_block_index].load();
    bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());

    if (!bottom_head_block) {
        return false;
    }

    SpruceTransVer::AdjSubsequentBlockFive* local_head_block_ptr = bottom_head_block;
    uint32_t get_index = 0;
    uint64_t temp_bitvector;

    recheck_bottom_lock:
    while (ptr_block->buffer_locks[index_in_64].load() != UNLOCKED);
    uint32_t timestamp_before = bottom_head_block->timestamp.load();
    // also recheck
    if (ptr_block->buffer_locks[index_in_64].load() == WRITE_LOCKED) {
        goto recheck_bottom_lock;
    }


    if (local_head_block_ptr->bitvector_64 != UINT64_MAX) {
        //value existed
        uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
        auto pre_bv = bottom_head_block->bitvector_64.load();
        auto type = local_head_block_ptr->type.load();
        if (type == 5) {
            //type = 5;
            //first check subsequent block
            for (int i = 0; i < 64; i++) {
                if (!get_bit(&pre_bv, i)) {
                    // first remove newly-added edges
                        if (local_head_block_ptr->adj_vertex[i].timestamp <= read_timestamp) {
                            neighbours.push_back({local_head_block_ptr->adj_vertex[i].des, local_head_block_ptr->adj_vertex[i].weight});
                        }
                }
            }
            //then check edge array
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            if (!edge_array) {
                block_size = 0;
                delete_space = 0;
            }
            else {
                block_size = edge_array[0];
                delete_space = edge_array[1];
            }
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(neighbours.size() + block_size - delete_space);
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);

            for (int i = 0; i < block_size; i++) {
                if (edge_info[i].des != UINT32_MAX) {
                    neighbours[new_index++] = {edge_info[i].des, property[i]};
                }
            }


        } else {
            //type = 4;
            for (int i = 0; i < (2 << type) ; i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back({local_head_block_ptr->adj_vertex[i].des, local_head_block_ptr->adj_vertex[i].weight});
                }
            }
        }
    } else {
        // check edge array
        if (!local_head_block_ptr->next_block.load()) {
            return false;
        }
        else {
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            block_size = edge_array[0];
            delete_space = edge_array[1];
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(neighbours.size() + block_size - delete_space);
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);

            for (int i = 0; i < block_size; i++) {
                if (edge_info[i].des != UINT32_MAX) {
                    neighbours[new_index++] = {edge_info[i].des, property[i]};
                }
            }
        }
    }




    uint32_t timestamp_after = bottom_head_block->timestamp.load();
    if (timestamp_before != timestamp_after) {
        goto restart;
    }

restart_snapshot:
    timestamp_before = bottom_head_block->timestamp.load();
    // check version
    if (local_head_block_ptr->timestamp > read_timestamp) {
        // need to read previous version



        // then restore updated edges/deleted edges
        auto log_block_ptr = (SpruceTransVer::LogBlock*)local_head_block_ptr->ptr_to_log_block.load();
        auto outer_offset = local_head_block_ptr->log_size / 64;
        for (int i = 0; i < outer_offset - 1; i++) {
            if (log_block_ptr->versioned_edges[BLOCK_SIZE-1].timestamp && TIMESTAMP_MASK <= read_timestamp) {
                continue;
            }
            for (int j = BLOCK_SIZE - 1; j >=0; j--) {
                if (log_block_ptr->versioned_edges[j].timestamp && TIMESTAMP_MASK > read_timestamp) {
                    // cases
                    auto current_type = (uint32_t) (log_block_ptr->versioned_edges[j].timestamp >> 56);
                    if (current_type == DELETE_TYPE) {
                        neighbours.push_back({log_block_ptr->versioned_edges[j].ver.des,log_block_ptr->versioned_edges[j].ver.weight});
                    }
                    else if (current_type == UPDATE_TYPE) {
                        if (neighbours.size() <= 64) {
                            for (int k = 0; k < neighbours.size(); k++) {
                                if (neighbours[k].des == log_block_ptr->versioned_edges[j].ver.des) {
                                    neighbours[k].weight = log_block_ptr->versioned_edges[j].ver.weight;
                                }
                            }
                        }
                        else {
                            for (int k = 0; k < 64; k++) {
                                if (neighbours[k].des == log_block_ptr->versioned_edges[j].ver.des) {
                                    neighbours[k].weight = log_block_ptr->versioned_edges[j].ver.weight;
                                }
                            }
                            // binary search
                            int64_t low = 64;
                            int64_t high = neighbours.size() - 1;
                            int64_t mid;
                            while (high >= low) {
                                mid = (low + high) / 2;
                                if(log_block_ptr->versioned_edges[j].ver.des < neighbours[mid].des){
                                    high = mid - 1;
                                }
                                else if (log_block_ptr->versioned_edges[j].ver.des == neighbours[mid].des) {
                                    neighbours[mid].weight = log_block_ptr->versioned_edges[j].ver.weight;
                                    break;
                                }
                                else {
                                    low = mid + 1;
                                }
                            }
                        }
                    }
                }
                else {
                    break;
                }
            }
            log_block_ptr = log_block_ptr->next_block;
        }
    }
    timestamp_after = bottom_head_block->timestamp.load();
    if (timestamp_before != timestamp_after) {
        goto restart_snapshot;
    }
    return true;
}

bool SpruceTransVer::get_neighbours_only(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
                                         std::vector<uint32_t> &neighbours) {
    //do not use locks
    uint32_t restart_num = 0;
    restart:
    neighbours.clear();
    uint64_t temp_ptr;
    auto from_node_id_low = (uint16_t) from_node_id;
    auto from_node_id_high = (uint16_t) (from_node_id >> 16);
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        return false;
    }
    temp_ptr = root->ptr_to_children[from_node_id_high].load();
    auto* middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp_ptr;
    if (!middle_block_ptr) {
        return false;
    }
    if (!get_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t)from_node_id_low)) {
        return false;
    }
    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t) (from_node_id_low % 64);

    SpruceTransVer::AdjSubsequentBlockFive* bottom_head_block;

    SpruceTransVer::PtrBlock* ptr_block = (SpruceTransVer::PtrBlock*)middle_block_ptr->ptr_to_children[ptr_block_index].load();
    bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());

    if (!bottom_head_block) {
        return false;
    }

    SpruceTransVer::AdjSubsequentBlockFive* local_head_block_ptr = bottom_head_block;
    uint32_t get_index = 0;
    uint64_t temp_bitvector;

    recheck_bottom_lock:
    if (restart_num < RESTART_THRESHOLD){
        restart_num++;
    }
    else {
        // read bottom head block exclusively
        return get_neighbours_only_exclusively(root, from_node_id, neighbours);
    }
    while (ptr_block->buffer_locks[index_in_64].load() != UNLOCKED);
    uint32_t timestamp_before = bottom_head_block->timestamp.load();
    // also recheck
    if (ptr_block->buffer_locks[index_in_64].load() == WRITE_LOCKED) {
        goto recheck_bottom_lock;
    }


    if (local_head_block_ptr->bitvector_64 != UINT64_MAX) {
        //value existed
        uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
        auto pre_bv = bottom_head_block->bitvector_64.load();
        auto type = local_head_block_ptr->type.load();
        if (type == 5) {
            //type = 5;
            //first check subsequent block
            for (int i = 0; i < 64; i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back(local_head_block_ptr->adj_vertex[i].des);
                }
            }

            //Edge tree version
//            temp_ptr = local_head_block_ptr->next_block.load();
//            auto edge_tree = (EdgeTree*)temp_ptr;
//            edge_tree->get_kvs(neighbours);


//            then check edge array
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            if (!edge_array) {
                block_size = 0;
                delete_space = 0;
            }
            else {
                block_size = edge_array[0];
                delete_space = edge_array[1];
            }
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(neighbours.size() + block_size - delete_space);
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);

            for (int i = 0; i < block_size; i++) {
                if (edge_info[i].des != UINT32_MAX) {
                    neighbours[new_index++] = edge_info[i].des;
                }
            }


        } else {
            //type = 4;
            for (int i = 0; i < (2 << type) ; i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back(local_head_block_ptr->adj_vertex[i].des);
                }
            }
        }
    } else {
        // check edge array
        if (!local_head_block_ptr->next_block.load()) {
            return false;
        }
        else {
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            block_size = edge_array[0];
            delete_space = edge_array[1];
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(neighbours.size() + block_size - delete_space);
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);

            for (int i = 0; i < block_size; i++) {
                if (edge_info[i].des != UINT32_MAX) {
                    neighbours[new_index++] = edge_info[i].des;
                }
            }
        }
    }
    uint32_t timestamp_after = bottom_head_block->timestamp.load();
    if (timestamp_before != timestamp_after) {
        if (restart_num < RESTART_THRESHOLD){
            restart_num++;
            goto restart;
        }
        else {
            // read bottom head block exclusively
            return get_neighbours_only_exclusively(root, from_node_id, neighbours);
        }
    }

    return true;
}

bool SpruceTransVer::get_neighbours_only_exclusively(SpruceTransVer::TopBlock* root, const uint32_t from_node_id,
                                         std::vector<uint32_t> &neighbours) {
    //do not use locks
    int restart_num = 0;
    restart:
    neighbours.clear();
    uint64_t temp_ptr;
    auto from_node_id_low = (uint16_t) from_node_id;
    auto from_node_id_high = (uint16_t) (from_node_id >> 16);
    if (!get_bit(&(root->bitmap_8kb), (uint32_t) from_node_id_high)) {
        return false;
    }
    temp_ptr = root->ptr_to_children[from_node_id_high].load();
    auto* middle_block_ptr = (SpruceTransVer::MiddleBlock*)temp_ptr;
    if (!middle_block_ptr) {
        return false;
    }
    if (!get_bit(&(middle_block_ptr->bitmap_8kb), (uint32_t)from_node_id_low)) {
        return false;
    }
    auto ptr_block_index = from_node_id_low / 64;
    auto auxiliary_ptr = reinterpret_cast<uint64_t*>(&middle_block_ptr->bitmap_8kb);
    uint64_t auxiliary_64 = auxiliary_ptr[ptr_block_index];
    uint64_t auxiliary_64_rev = __builtin_bswap64(auxiliary_64);
    auto ptr_num = __builtin_popcountl(auxiliary_64);
    auto index_in_64 = (uint32_t) (from_node_id_low % 64);

    SpruceTransVer::AdjSubsequentBlockFive* bottom_head_block;

    SpruceTransVer::PtrBlock* ptr_block = (SpruceTransVer::PtrBlock*)middle_block_ptr->ptr_to_children[ptr_block_index].load();
    bottom_head_block = (SpruceTransVer::AdjSubsequentBlockFive*)(ptr_block->ptr_to_buffer[index_in_64].load());

    if (!bottom_head_block) {
        return false;
    }

    SpruceTransVer::AdjSubsequentBlockFive* local_head_block_ptr = bottom_head_block;
    uint32_t get_index = 0;
    uint64_t temp_bitvector;
    uint8_t unlocked_m = UNLOCKED;
    uint8_t write_locked_m = WRITE_LOCKED;
    while(!ptr_block->buffer_locks[index_in_64].compare_exchange_strong(unlocked_m, write_locked_m)) {
        unlocked_m = UNLOCKED;
    }

    if(!local_head_block_ptr || local_head_block_ptr->obsolete_flag) {
        if (restart_num > RESTART_THRESHOLD) {
            ptr_block->buffer_locks[index_in_64]--;
            return false;
        }
        else {
            restart_num++;
            ptr_block->buffer_locks[index_in_64]--;
            goto restart;
        }
    }


    if (local_head_block_ptr->bitvector_64 != UINT64_MAX) {
        //value existed
        uint64_t temp_bv_rev = __builtin_bswap64(local_head_block_ptr->bitvector_64.load());
        auto pre_bv = bottom_head_block->bitvector_64.load();
        auto type = local_head_block_ptr->type.load();
        if (type == 5) {
            //type = 5;
            //first check subsequent block
            for (int i = 0; i < 64; i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back(local_head_block_ptr->adj_vertex[i].des);
                }
            }

            //Edge tree version
//            temp_ptr = local_head_block_ptr->next_block.load();
//            auto edge_tree = (EdgeTree*)temp_ptr;
//            edge_tree->get_kvs(neighbours);


//            then check edge array
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            if (!edge_array) {
                block_size = 0;
                delete_space = 0;
            }
            else {
                block_size = edge_array[0];
                delete_space = edge_array[1];
            }
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(neighbours.size() + block_size - delete_space);
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);

            for (int i = 0; i < block_size; i++) {
                if (edge_info[i].des != UINT32_MAX) {
                    neighbours[new_index++] = edge_info[i].des;
                }
            }


        } else {
            //type = 4;
            for (int i = 0; i < (2 << type) ; i++) {
                if (!get_bit(&pre_bv, i)) {
                    neighbours.push_back(local_head_block_ptr->adj_vertex[i].des);
                }
            }
        }
    } else {
        // check edge array
        if (!local_head_block_ptr->next_block.load()) {
            ptr_block->buffer_locks[index_in_64]--;
            return false;
        }
        else {
            temp_ptr = local_head_block_ptr->next_block.load();
            auto edge_array = (uint32_t*)temp_ptr;
            uint32_t block_size, delete_space;
            block_size = edge_array[0];
            delete_space = edge_array[1];
            uint32_t new_index = neighbours.size(); // use new index as array index to improve efficiency (instead of push_back)
            neighbours.resize(neighbours.size() + block_size - delete_space);
            auto edges_ptr = (SpruceTransVer::WeightedOutEdge*)(edge_array + 2);
            auto edge_info = (DesT*)(edges_ptr);
            auto property = (double*)(edge_info + block_size);

            for (int i = 0; i < block_size; i++) {
                if (edge_info[i].des != UINT32_MAX) {
                    neighbours[new_index++] = edge_info[i].des;
                }
            }
        }
    }
    ptr_block->buffer_locks[index_in_64]--;
    return true;
}

//std::unique_ptr<EdgeTree::Node> EdgeTree::createNode() {
//    return std::make_unique<Node>();
//}
//
//std::unique_ptr<EdgeTree::LeafNode> EdgeTree::createLeafNode() {
//    return std::make_unique<LeafNode>();
//}


EdgeTree::EdgeTree() {
    root = createNode();
}

std::unique_ptr<EdgeTree::Node> EdgeTree::createNode() {
    return std::make_unique<InternalNode>();
}

std::unique_ptr<EdgeTree::LeafNode> EdgeTree::createLeafNode() {
    return std::make_unique<LeafNode>();
}

void EdgeTree::insert(uint32_t key, EdgeProperty value) {
    insertHelper(root.get(), key, value, 0);
}

std::vector<EdgeProperty> EdgeTree::get(uint32_t key) {
    return getHelper(root.get(), key, 0);
}

void EdgeTree::insertHelper(Node* node, uint32_t key, EdgeProperty value, int level) {
    int shiftAmount = 24 - (level * 8);
    uint8_t index = (key >> shiftAmount) & 0xFF;
    int count;

    if (level == 3) {
        // We are at the leaf level, cast the node to a LeafNode and insert the value
        LeafNode* leafNode = static_cast<LeafNode*>(node);
        leafNode->bitmap.set(index);

        // Find the correct position to insert the value
        auto it = leafNode->values.begin();
        count = 0;
        std::bitset<64> bitset64[4];
        for (int i = 0; i < 4; ++i) {
            for (int j = 0; j < 64; ++j) {
                bitset64[i].set(j, leafNode->bitmap.test(i * 64 + j));
            }
        }
        for (int i = 0; i < 4 && i * 64 <= index; ++i) {
            count += __builtin_popcountll(bitset64[i].to_ullong() & ((1ULL << std::min(64, index - i * 64)) - 1));
        }
        std::advance(it, count);
        leafNode->values.insert(it, value);
    } else {
        // We are not at the leaf level
        InternalNode* internalNode = static_cast<InternalNode*>(node);
        if (!internalNode->bitmap.test(index)) {
            // The child does not exist, so create it
            internalNode->bitmap.set(index);

            // Find the correct position to insert the child internalNode
            auto it = internalNode->children.begin();
            count = 0;
            std::bitset<64> bitset64[4];
            for (int i = 0; i < 4; ++i) {
                for (int j = 0; j < 64; ++j) {
                    bitset64[i].set(j, internalNode->bitmap.test(i * 64 + j));
                }
            }
            for (int i = 0; i < 4 && i * 64 <= index; ++i) {
                count += __builtin_popcountll(bitset64[i].to_ullong() & ((1ULL << std::min(64, index - i * 64)) - 1));
            }
            std::advance(it, count);
            internalNode->children.insert(it, level == 2 ? createLeafNode() : createNode());
        }
        else {
            count = 0;
            std::bitset<64> bitset64[4];
            for (int i = 0; i < 4; ++i) {
                for (int j = 0; j < 64; ++j) {
                    bitset64[i].set(j, internalNode->bitmap.test(i * 64 + j));
                }
            }
            for (int i = 0; i < 4 && i * 64 <= index; ++i) {
                count += __builtin_popcountll(bitset64[i].to_ullong() & ((1ULL << std::min(64, index - i * 64)) - 1));
            }
        }


        // Recurse to the next level
        insertHelper(internalNode->children[count].get(), key, value, level + 1);
    }
}


std::vector<EdgeProperty> EdgeTree::getHelper(Node* node, uint32_t key, int level) {
    int shiftAmount = 24 - (level * 8);
    uint8_t index = (key >> shiftAmount) & 0xFF;

    if (!node->bitmap.test(index)) {
        // The key does not exist in the tree
        return {};
    }

    if (level == 3) {
        // We are at the leaf level, cast the node to a LeafNode and return the value
        LeafNode* leafNode = static_cast<LeafNode*>(node);
        return {leafNode->values[__builtin_popcount(leafNode->bitmap.to_ulong() & ((1 << index) - 1))]};
    } else {
        // We are not at the leaf level, so recurse to the next level
        InternalNode* internalNode = static_cast<InternalNode*>(node);
        return getHelper(internalNode->children[__builtin_popcount(internalNode->bitmap.to_ulong() & ((1 << index) - 1))].get(), key, level + 1);
    }
}


void EdgeTree::getAllKeysAndValues(Node* node, uint32_t currentKey, int level, std::vector<SpruceTransVer::WeightedOutEdgeSimple>& results) {

    if (level == 3) { // We're at a leaf node
        LeafNode* leaf = dynamic_cast<LeafNode*>(node);
        int j = 0;
        for (int i = 0; i < 256; ++i) {
            if (leaf->bitmap.test(i)) {
                uint32_t newKey = (currentKey << 8) | i; // Append the new 8 bits to the key
                results.push_back({newKey, static_cast<float>(leaf->values[j].weight)});
                j++;
            }
        }

    }
    else {

        int j = 0;
        // It's an internal node
        InternalNode* internal = static_cast<InternalNode*>(node);
        for (int i = 0; i < 256; ++i) {
            if (node->bitmap.test(i)) {
                uint32_t newKey = (currentKey << 8) | i; // Append the new 8 bits to the key
                getAllKeysAndValues(internal->children[j].get(), newKey, level + 1, results);
                j++;
            }
        }
    }

//    return results;
}
