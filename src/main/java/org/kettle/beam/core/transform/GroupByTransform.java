package org.kettle.beam.core.transform;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;

public class GroupByTransform extends PTransform<PCollection<KettleRow>, PCollection<KettleRow>> {

  @Override public PCollection<KettleRow> expand( PCollection<KettleRow> input ) {

    // TODO: Group over Kettle values in rows ...
    //

    return input;
  }
}
