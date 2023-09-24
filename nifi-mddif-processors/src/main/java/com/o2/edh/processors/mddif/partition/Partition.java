package com.o2.edh.processors.mddif.partition;

import java.util.Arrays;

public class Partition {
    private PartitionObj partitionArr[];

    public Partition(PartitionObj[] partitionArr) {
        partitionArr = partitionArr;
    }

    public PartitionObj[] getpartitionArr() {
        return partitionArr;
    }

    public void setpartitionArr(PartitionObj[] partitionArr) {
        partitionArr = partitionArr;
    }

    @Override
    public String toString() {
        return "Partition{" +
                "partitionArr=" + Arrays.toString(partitionArr) +
                '}';
    }
}

