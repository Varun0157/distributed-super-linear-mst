An implementation of the superlinear MPC algorithm of distributed MST calculation, using MapReduce primitives. A single map+reduce step is considered equivalent to a round of the MPC algorithm.

#### Why?

To help bridge the gap between algorithms that are generally theoretically studied, into simple, practical implementations.

#### Run

Package the given code into a jar that can be run on the underlying hadoop file system:

```sh
mvn clean package
```

Run the code on a given graph:

```sh
hadoop jar target/mst-1.0-SNAPSHOT-jar-with-dependencies.jar <input graph> <base-path> <input-dir> <output-prefix> <epsilon>
```

For an example, see the [run script](./scripts/run.sh).

_This was run on the RCE server in IIIT-Hyderabad after loading the `hdfs` module._

> [!NOTE]  
> The graph is initially loaded into memory completely in order to split it into the required nodes. This is the only point at which any given process may exceed the memory constraint set by n and epsilon.
