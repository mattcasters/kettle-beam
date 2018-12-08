package org.kettle.beam.transform;

import org.junit.Test;
import org.kettle.beam.util.BeamTransMetaUtil;
import org.pentaho.di.trans.TransMeta;

public class MergeJoinPipelineTest extends PipelineTestBase {

  @Test
  public void testMergeJoinPipeline() throws Exception {

    TransMeta transMeta = BeamTransMetaUtil.generateMergeJoinTransMeta(
      "inputs-merge-join-output",
      "INPUT",
      "OUTPUT",
      metaStore
    );

    try {
      createRunPipeline( transMeta );
    } catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

}