package main

import (
	"context"
	"fmt"
	comms "mst/superlinear/comms"
	utils "mst/superlinear/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func sendEdges(edges []utils.Edge, senderAddr string, receiverAddr string) error {
	conn, err := grpc.NewClient(receiverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("unable to create client connection: %v", err)
	}
	defer conn.Close()

	client := comms.NewEdgeDataServiceClient(conn)
	edgeData := make([]*comms.EdgeData, 0)
	for _, edge := range edges {
		edgeData = append(edgeData, &comms.EdgeData{
			Src:    int32(edge.Src),
			Dest:   int32(edge.Dest),
			Weight: int32(edge.Weight),
		})
	}

	req := &comms.EdgeDataRequest{Edges: edgeData, Addr: senderAddr}

	ctx, cancel := context.WithTimeout(context.Background(), utils.RpcTimeout())
	defer cancel()

	resp, err := client.SendEdgeData(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send edge data: %v", err)
	}
	if !resp.Success {
		return fmt.Errorf("failed to send edge data")
	}

	return nil
}
