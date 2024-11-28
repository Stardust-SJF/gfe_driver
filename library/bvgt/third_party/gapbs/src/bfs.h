//
// Created by sjf on 8/22/2022.
//

#ifndef GRAPHINDEX_BFS_H
#define GRAPHINDEX_BFS_H

#include <iostream>
#include <vector>

#include "benchmark.h"
#include "bitmap.h"
#include "builder.h"
#include "command_line.h"
#include "graph.h"
#include "platform_atomics.h"
#include "pvector.h"
#include "sliding_queue.h"
#include "timer.h"
#include "../../../spruce_transaction.h"

extern int64_t BUStep(const Graph &g, pvector<NodeID> &parent, Bitmap &front,
               Bitmap &next);
extern int64_t TDStep(SpruceTransVer::TopBlock *g, pvector<NodeID> &parent,
               SlidingQueue<NodeID> &queue);
extern void QueueToBitmap(const SlidingQueue<NodeID> &queue, Bitmap &bm);
extern void BitmapToQueue(const Graph &g, const Bitmap &bm,
                  SlidingQueue<NodeID> &queue);
extern pvector<NodeID> InitParent(int vertex_num);
extern pvector<NodeID> DOBFS(SpruceTransVer::TopBlock* g/*const Graph &g*/, NodeID source, int vertex_num, int edge_num, int src_out_degree, int alpha = 15,
                      int beta = 18);
extern void PrintBFSStats(const Graph &g, const pvector<NodeID> &bfs_tree);
extern bool BFSVerifier(const Graph &g, NodeID source,
                 const pvector<NodeID> &parent);

#endif //GRAPHINDEX_BFS_H
