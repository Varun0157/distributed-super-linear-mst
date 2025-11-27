package utils

import (
	"fmt"
	"sync"
	"time"
)

func CreatePartitionGenerator(numItems int, numPartitions int) func() (int, int, error) {
	partitionMutex := sync.Mutex{}

	size := numItems / numPartitions
	extra := numItems % numPartitions

	start := 0

	getNextPartition := func() (int, int, error) {
		partitionMutex.Lock()
		defer partitionMutex.Unlock()

		if start >= numItems {
			return 0, 0, fmt.Errorf("no more partitions available")
		}

		pStart := start

		pEnd := pStart + size
		if extra > 0 {
			pEnd += 1
			extra -= 1
		}

		start = pEnd
		return pStart, pEnd, nil
	}

	return getNextPartition
}

func Partition[T any](data []T, numPartitions int) ([][]T, error) {
	if numPartitions < 1 {
		return nil, fmt.Errorf("number of partitions must be at least 1")
	}
	if numPartitions > len(data) {
		return nil, fmt.Errorf("number of partitions cannot exceed length of slice")
	}

	result := make([][]T, numPartitions)

	createPartition := CreatePartitionGenerator(len(data), numPartitions)

	for i := 0; i < numPartitions; i++ {
		start, end, err := createPartition()
		if err != nil {
			return nil, fmt.Errorf("failed to create partition: %v", err)
		}
		result[i] = data[start:end]
	}

	return result, nil
}

func RpcTimeout() time.Duration {
	return 120 * time.Second
}
