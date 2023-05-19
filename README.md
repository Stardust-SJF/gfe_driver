---
GFE Driver
---

The GFE (Graph Framework Evaluation) Driver is the program used to run the experiments in "Spruce: a Fast, Space-saving Structure for Dynamic Graph Storage", measuring the throughput of updates in libraries supporting structural dynamic graphs and the completion times of the [Graphalytics kernels](https://github.com/ldbc/ldbc_graphalytics). 
The driver supports the following structures: Spruce, [Sortledton](https://gitlab.db.in.tum.de/per.fuchs/sortledton), [Teseo](https://github.com/cwida/teseo), [GraphOne](https://github.com/the-data-lab/GraphOne), 
[Stinger](http://stingergraph.com/) and [LiveGraph](https://github.com/thu-pacman/LiveGraph-Binary). 
It can run several kinds experiments: insert/delete all edges in a random permuted order from an input graph, 
execute the updates specified by a [graphlog file](https://github.com/whatsthecraic/graphlog) and run the kernels of the Graphalytics suite: BFS, PageRank (PR), local triangle counting (LCC), weighted shortest paths (SSSP), weakly connected components (WCC) and community detection through label propagation (CDLP). 
The core codes of Spruce could be found at [Spruce](https://github.com/Stardust-SJF/Spruce). 

### Build 

#### Requisites 
- O.S. Linux
- Autotools, [Autoconf 2.69+](https://www.gnu.org/software/autoconf/)
- A C++17 compliant compiler with support for OpenMP. We tested it with GCC 10.
- libnuma 2.0 +
- [libpapi 5.5 +](http://icl.utk.edu/papi/)
- [SQLite 3.27 +](https://sqlite.org)
- Intel Threading Building Blocks 2 (version 2020.1-2)
- Disable NUMA balancing feature to avoid the Linux Kernel to swap pages during insertions: `echo 0 | sudo tee  /proc/sys/kernel/numa_balancing`

#### Configure

Initialise the sources and the configure script by:

```
git clone https://github.com/PerFuchs/gfe_driver
cd gfe_driver
git submodule update --init
mkdir build && cd build
autoreconf -iv ..
```

The driver needs to be linked with the system to evaluate, which has to be built ahead. 
We do not recommend linking the driver with multiple systems at once, 
due to the usage of global variables in some systems and other naming clashes. 
Instead, it is safer to reconfigure and rebuild the driver each time for a single specific system.


##### Stinger
Use the branch `feature/gfe `, it contains additional patches w.r.t. 
[upstream](https://github.com/stingergraph/stinger), from https://github.com/whatsthecraic/stinger.
For the paper, we evaluted commit "2bcfac38785081c7140b0cd27f3aecace088d664"

```
git clone https://github.com/whatsthecraic/stinger -b feature/gfe
cd stinger
mkdir build && cd stinger
cmake ../ -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=0 
make
```
If the build has been successful, it should at least create the executable `bin/stinger_server`.

Configure the GFE driver with:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-stinger=/path/to/stinger/build
```


##### GraphOne

Use the branch `feature/gfe `, it contains additional patches w.r.t.
[upstream](https://github.com/the-data-lab/GraphOne), from https://github.com/whatsthecraic/GraphOne.
For the paper, we evaluated "1475bf5887aaf37dd7aa47377e9f11a94aa0d880".

```
git clone https://github.com/whatsthecraic/GraphOne -b feature/gfe
cd GraphOne
mkdir build && cd build
cmake -S ../ -DCMAKE_BUILD_TYPE=Release
make -j
```
If the build has been successful, it should at least create the executable `graphone64`.
Then, configure the driver with:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-graphone=/path/to/graphone/build
```

##### LiveGraph

Download the binary library from the [official repository](https://github.com/thu-pacman/LiveGraph-Binary/releases). 
In the paper, we evaluated version 20200829.
Then configure the driver by pointing the path to where the library has been downloading:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-livegraph=/path/to/livegraph/lib
```

##### Teseo

Use the branch `master` from https://github.com/cwida/teseo.
In the paper, we evaluated version `14227577731d6369b5366613f3e4a679b1fd7694`.

```
git clone https://github.com/cwida/teseo
cd teseo
./autoreconf -iv
mkdir build && cd build
../configure --enable-optimize --disable-debug
make -j
```

If the build has been successful, it should at least create the archive `libteseo.a`.
Then configure the driver with:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-teseo=/path/to/teseo/build   
```

##### Sortledton
Use the branch `master` from `https://gitlab.db.in.tum.de/per.fuchs/sortledton`.
For the paper, we evaluated commit "a32b8ac208bb889b518e14b1317957c9a8c466b6".

Follow the instructions in the README of the repository to setup and build the library.
Then configure the driver with:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-sortledton=/path/to/microbenchmark/build  
```

##### Spruce

Use the library in our GitHub Release Page:

Then configure the driver with:

````````shell
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-bvgt=/path/to/spruce/build/
````````

#### Compile

Once configured, run `make -j`. There is no `install` target, the final artifact is the executable `gfe_driver`. 

If in the mood of running the testsuite, type `make check -j`.

### Datasets

In our experiments, we used the following input graphs and data sets:

- `dota-league` and `graph500-SF`, with `SF` in {24 26} , were taken from the [official Graphalytics collection](https://www.graphalytics.org/datasets).
- `uniform-SF`, with `SF` in {24, 26} were generated with an [ad-hoc tool](https://github.com/whatsthecraic/uniform_graph_generator). These are synthetic graphs having the same number of vertices and edges of `graph500-SF`, but a uniform node degree distribution.
- The logs for the experiments with updates, i.e. with both insertions and deletions,
  were generated with another [ad-hoc tool](https://github.com/whatsthecraic/graphlog). 
- `yahoo-songs` was taken from the [Konect webpage](http://konect.cc/networks/) they were prepared 
  for our experiments by sorting them by timestamp and removing duplicates by using `tools/timestampd_graph_2_edge_list.py`.  
- `livejournal`,`orkut` and `com-friendster` were taken from the [Stanford Large Network Dataset Collection](https://snap.stanford.edu/data/index.html).

### Executing the driver


The driver takes as input a list of options together with a graph, and emits the results into a sqlite3 database.
There are three kinds of experiments that can be executed:

- **Insertions only** : insert all vertices and edges from an input graph, in a random order. Use the command:

```
./gfe_driver -G /path/to/input/graph.properties -u -l <system_to_evaluate> -w <num_threads> -d output_results.sqlite3
```
- **Deletions** : insert all vertices and edges from an input graph, in a random order, then delete all of them in random order. 
Comment line 198~231 in experiment/details/insert_only.cpp, then recompile the gfe-driver and use following command to evaluate deletion performance:

```
./gfe_driver -G /path/to/input/graph.properties -u -l <system_to_evaluate> -w <num_threads> -d output_results.sqlite3
```

- **Updates**: perform all insertions and deletions from a log. Add the option --log /path/to/updates.graphlog :

```
./gfe_driver -G /path/to/input/graph.properties -u --log /path/to/updates.graphlog --aging_timeout 24h -l <system_to_evaluate> -w <num_threads> -d output_results.sqlite3
```

Following parameters can be used to record memory footprint during the update process:

```shell
-aging_memfp --aging_memfp_physical --aging_memfp_threshold 500G --aging_release_memory=false
```



The option `--aging_timeout` serves to limit the total time to execute the experiment.
The option `--aging_memfp` records the memory footprint as the experiment proceeds.
The option `--aging_memfp_physical` records the physical memory (RSS) of the process rather than the virtual memory of the glibc allocator. 
The option`--aging_memfp_threshold 500G` terminates the experiment if the memory footprint measured is greater than 330 GB .
The option `--aging_release_memory=false` avoids releasing the memory used in the driver to load the graph from the file, as it may (or may not) recycled by the libraries. 

- **Graphalytics**: execute the six kernels from the Graphalytics suite. Add the option `-R <N>` to repeat `N` times the execution of all Graphalytics kernels, one after the other. E.g., to run the kernels five times, after all vertices and edges have been inserted, use:

```
./gfe_driver -G /path/to/input/graph.properties -u -l <system_to_evaluate> -w <num_threads> -R 5 -d output_results.sqlite3
```

Type `./gfe_driver -h` for the full list of options and for the libraries that can be evaluated (option `-l`). The driver spawns the number of threads given by the option `-w` to concurrently run all insertions or updates. For Graphalytics, it defaults to the total number of the physical threads in the machine. This setting can be changed with the option `-r <num_threads>`. Note that the numbers
in the library codes (e.g. teseo.**6**, stinger**3**) are unrelated to the versions of the systems evaluated, they were only used
internally for development purposes.

The database `output_results.sqlite3` will contain the final results. 

### Repeating the experiments

These are the full commands to repeat the experiments in the paper:

##### Random Insertions

```bash
./gfe_driver -u -G /path/to/input/graph.properties -l stinger7-ref -w 56
./gfe_driver -u -G /path/to/input/graph.properties -l g1_v6-ref-ignore-build -w 56
./gfe_driver -u -G /path/to/input/graph.properties -l livegraph3_ro -w 56
./gfe_driver -u -G /path/to/input/graph.properties -l teseo.13 -w 56
./gfe_driver -u -G /path/to/input/graph.properties -l sortledton.4 -w 56
./gfe_driver -u -G /path/to/input/graph.properties -l bvgt -w 56
```

##### Sequential Insertions

```bash
./gfe_driver -u -G /path/to/input/graph -l stinger7-ref -w 56 --is_timestamped true
./gfe_driver -u -G /path/to/input/graph -l g1_v6-ref-ignore-build -w 56 --is_timestamped true
./gfe_driver -u -G /path/to/input/graph -l livegraph3_ro -w 56 --is_timestamped true
./gfe_driver -u -G /path/to/input/graph -l teseo.13 -w 56 --is_timestamped true
./gfe_driver -u -G /path/to/input/graph -l sortledton.4 -w 56 --is_timestamped true
./gfe_driver -u -G /path/to/input/graph -l bvgt -w 56 --is_timestamped true
```

##### Memory Consumption

Comment line 92~99 in experiment/details/insert_only.cpp, then recompile the gfe-driver and use following command to evaluate deletion performance. The memory consumption during the loading process will be printed in the console.

```shell
./gfe_driver -u -G /path/to/input/graph.properties -l stinger7-ref -w 56 >> result.txt
./gfe_driver -u -G /path/to/input/graph.properties -l g1_v6-ref-ignore-build -w 56  >> result.txt
./gfe_driver -u -G /path/to/input/graph.properties -l livegraph3_ro -w 56  >> result.txt
./gfe_driver -u -G /path/to/input/graph.properties -l teseo.13 -w 56 >> result.txt
./gfe_driver -u -G /path/to/input/graph.properties -l sortledton.4 -w 56  >> result.txt
./gfe_driver -u -G /path/to/input/graph.properties -l bvgt -w 56  >> result.txt
```

##### Graph Analytics

```shell
./gfe_driver  -G /path/to/input/graph.properties -u -l csr3 --load -r 56 -R 5 --blacklist lcc,cdlp -d result.sqlite3
./gfe_driver  -G /path/to/input/graph.properties -u -l csr3-lcc --load -r 56 -R 5 --blacklist cdlp -d result.sqlite3
./gfe_driver  -G /path/to/input/graph.properties -u -l stinger7-ref -w 56 -r 56 -R 5 --blacklist cdlp -d result.sqlite3
./gfe_driver  -G /path/to/input/graph.properties -u -l g1_v6-ref-ignore-build -w 56 -r 56 -R 5 --blacklist cdlp -d results_orkut.sqlite3
./gfe_driver  -G /path/to/input/graph.properties -u -l livegraph3_ro -w 56 -r 56 -R 5 --blacklist cdlp -d result.sqlite3
./gfe_driver  -G /path/to/input/graph.properties -u -l teseo.13 -w 56 -r 56 -R 5 --blacklist cdlp -d result.sqlite3
./gfe_driver  -G /path/to/input/graph.properties -u -l sortledton.4 -w 56 -r 56 -R 5 --blacklist cdlp -d result.sqlite3
./gfe_driver  -G /path/to/input/graph.properties -u -l bvgt -w 56 -r 56 -R 5 --blacklist cdlp -d result.sqlite3
```

##### Scalability 

For `graph500-24` and p in {1,2,4,8,14,28,42,56} and 5 runs.

```bash
./gfe_driver  -u  -R 0 -d ./result.sqlite3 -l stinger7-ref -G /path/to/input/graph.properties -w p
./gfe_driver  -u  -R 0 -d ./result.sqlite3 -l g1_v6-ref-ignore-build -G /path/to/input/graph.properties -w p
./gfe_driver  -u  -R 0 -d ./result.sqlite3 -l livegraph3_ro -G /path/to/input/graph.properties -w p
./gfe_driver  -u  -R 0 -d ./result.sqlite3 -l teseo.13 -G /path/to/input/graph.properties -w p
./gfe_driver  -u  -R 0 -d ./result.sqlite3 -l sortledton.4 -G /path/to/input/graph.properties -w p
./gfe_driver  -u  -R 0 -d ./result.sqlite3 -l bvgt -G /path/to/input/graph.properties -w p
```

##### Mixed updates and analytics (Figure 14)

For all combinations of reading ($r in \[1, 2, 4, 8, 16, 32\]) and writing threads ($w in \[16, 48\]).
```bash
./gfe_driver  -u  -R 3 -d results.sqlite3 -l bvgt -G /path/to/graph500-24.properties -w $w -r $r --blacklist sssp,cdlp,pagerank,wcc,lcc --log /path/to/graph500-24-1.0.graphlog --aging_timeout 2h --mixed_workload true --block_size 512
./gfe_driver  -u  -R 3 -d results.sqlite3 -l livegraph3_ro -G /path/to/graph500-24.properties -w $w -r $r --blacklist sssp,cdlp,pagerank,wcc,lcc --log /path/to/graph500-24-1.0.graphlog --aging_timeout 2h --mixed_workload true
./gfe_driver  -u  -R 3 -d results.sqlite3 -l bvgt -G /path/to/graph500-24.properties -w $w -r $r --blacklist sssp,cdlp,bfs,wcc,lcc --log /path/to/graph500-24-1.0.graphlog --aging_timeout 2h --mixed_workload true --block_size 512
./gfe_driver  -u  -R 3 -d results.sqlite3 -l livegraph3_ro -G /path/to/graph500-24.properties -w $w -r $r --blacklist sssp,cdlp,bfs,wcc,lcc --log /path/to/graph500-24-1.0.graphlog --aging_timeout 2h --mixed_workload true
```
