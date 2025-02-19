package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"slices"

	edgeDataComms "mst/superlinear/comms"
	utils "mst/superlinear/utils"

	"google.golang.org/grpc"
)

type addrDetails struct {
	nodes []string
	index int
}

type FilteringServer struct {
	edgeDataComms.UnimplementedEdgeDataServiceServer

	md          *MetaData
	grpcServer  *grpc.Server
	addrDetails addrDetails
	edgeData    *ReceivedEdgeData
	round       int
}

func NewFilteringServer(lis net.Listener, nodes []string, edges []utils.Edge, md *MetaData) (*FilteringServer, error) {
	addr := lis.Addr().String()
	index := slices.Index(nodes, addr)
	if index < 0 {
		return nil, fmt.Errorf("address %s not found in list %v", addr, nodes)
	}

	partitionedEdges, err := utils.Partition(edges, len(nodes))
	if err != nil {
		return nil, fmt.Errorf("%s - unable to partition edge list: %v", addr, err)
	}

	s := &FilteringServer{
		md: md,
		grpcServer: grpc.NewServer(
			grpc.MaxSendMsgSize(math.MaxInt64),
			grpc.MaxRecvMsgSize(math.MaxInt64),
		),
		addrDetails: addrDetails{
			index: index,
			nodes: nodes,
		},
		edgeData: NewReceivedEdgeData(),
		round:    0,
	}
	edgeDataComms.RegisterEdgeDataServiceServer(s.grpcServer, s)
	log.Printf("%s - registered server", s.addrDetails.nodes[s.addrDetails.index])

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Fatalf("%s - failed to serve: %v", s.addrDetails.nodes[s.addrDetails.index], err)
		}
	}()

	log.Printf("%s - server started", s.addrDetails.nodes[s.addrDetails.index])

	for i := range nodes {
		err := s.addEdges(partitionedEdges[i], nodes[i])
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *FilteringServer) ShutDown() {
	s.grpcServer.GracefulStop()
	log.Printf("%s(%d) - server shut down", s.addrDetails.nodes[s.addrDetails.index], s.round)
}

func (s *FilteringServer) addEdges(edges []utils.Edge, addr string) error {
	if err := s.edgeData.addEdges(edges, addr); err != nil {
		return err
	}

	log.Printf("%s(%d) - received %d edges from %s", s.addrDetails.nodes[s.addrDetails.index], s.round, len(edges), addr)

	return nil
}

// rpc
func (s *FilteringServer) SendEdgeData(ctx context.Context, req *edgeDataComms.EdgeDataRequest) (*edgeDataComms.EdgeDataResponse, error) {
	edges := make([]utils.Edge, 0)
	for _, edge := range req.Edges {
		edges = append(edges, utils.Edge{
			Src:    int(edge.Src),
			Dest:   int(edge.Dest),
			Weight: int(edge.Weight),
		})
	}
	addr := req.Addr

	if err := s.addEdges(edges, addr); err != nil {
		return &edgeDataComms.EdgeDataResponse{Success: false}, err
	}
	return &edgeDataComms.EdgeDataResponse{Success: true}, nil
}
