An MPC implementation of the distributed super-linear MST, inspired by _Lattanzi et al._'s MRC model as defined in [Filtering](./docs/spaa11-matchings.pdf). 

#### Run 
Step into the source directory, and set up the required dependencies (primarily related to gRPC):
```sh
cd src 
go mod tidy
cd - 
```

In order to automatically generate a large graph, generate a ground truth using a sequentia Kruskal's algorithm, generate the graph as predicted by *Filtering*, and compare the two, use the [test script](./scripts/test.sh). 

```sh
bash scripts/test.sh
```

> [!NOTE]  
> The graph is initially loaded into memory completely in order to split it into the required nodes. This is the only point at which any given process may exceed the memory constraint set by n and epsilon. 

#### TODO 
- [ ] diagram of methodology 
