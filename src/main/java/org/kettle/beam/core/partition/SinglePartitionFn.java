package org.kettle.beam.core.partition;

import org.apache.beam.sdk.transforms.Partition;
import org.kettle.beam.core.KettleRow;

public class SinglePartitionFn implements Partition.PartitionFn<KettleRow> {

  private static final long serialVersionUID = 95100000000000001L;

  @Override public int partitionFor( KettleRow elem, int numPartitions ) {
    return 0;
  }
}
