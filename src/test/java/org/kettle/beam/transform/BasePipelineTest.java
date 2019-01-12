package org.kettle.beam.transform;

import org.junit.Test;
import org.kettle.beam.util.BeamTransMetaUtil;
import org.pentaho.di.trans.TransMeta;

public class BasePipelineTest extends PipelineTestBase {

  @Test
  public void testBasicPipeline() throws Exception {

    TransMeta transMeta = BeamTransMetaUtil.generateBeamInputOutputTransMeta(
      "io-dummy-output",
      "INPUT",
      "OUTPUT",
      metaStore
    );

    createRunPipeline( transMeta );
  }
}