# test from super-linear dir

echo "generating large graph"
python scripts/create-graph.py 10000 500000 data/graph.txt

echo && echo "generating ground truth using sequential kruskal's"
python scripts/kruskals.py data/graph.txt >res/kruskals.txt

echo && echo "generating results of filtering algorithm"
cd src || exit
go run ./*.go ../data/graph.txt ../res/filtering.txt 0.1
cd - || exit

echo && echo "comparing results ... "
numEdgesFiltering=$(($(wc -l <res/filtering.txt) - 1))
numEdgesKruskals=$(($(wc -l <res/kruskals.txt) - 1))
echo "numEdgesFiltering: $numEdgesFiltering"
echo "numEdgesKruskals: $numEdgesKruskals"

# get weights from the last line of each file
filteringLastLine=$(tail -n 1 res/filtering.txt)
kruskalsLastLine=$(tail -n 1 res/kruskals.txt)
echo "filteringLastLine: $filteringLastLine"
echo "kruskalsLastLine: $kruskalsLastLine"
