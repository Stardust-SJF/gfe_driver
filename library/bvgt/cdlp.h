//
// Created by sjf on 4/18/2022.
//

#ifndef GRAPHINDEX_GRAPH_ALGORITHMS_H
#define GRAPHINDEX_GRAPH_ALGORITHMS_H

#include "spruce_transaction.h"


std::unique_ptr<uint32_t[]> cdlp_m(SpruceTransVer::TopBlock* g, uint32_t max_iterations, uint32_t num_vertices);

#endif //GRAPHINDEX_GRAPH_ALGORITHMS_H
