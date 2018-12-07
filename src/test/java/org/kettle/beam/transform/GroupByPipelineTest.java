package org.kettle.beam.transform;

import org.junit.Test;
import org.kettle.beam.util.BeamTransMetaUtil;
import org.pentaho.di.trans.TransMeta;

public class GroupByPipelineTest extends PipelineTestBase {

  @Test
  public void testGroupByPipeline() throws Exception {

    TransMeta transMeta = BeamTransMetaUtil.generateBeamGroupByTransMeta(
      "input-group-output",
      "INPUT",
      "OUTPUT",
      metaStore
    );

    createRunPipeline( transMeta );
  }
}