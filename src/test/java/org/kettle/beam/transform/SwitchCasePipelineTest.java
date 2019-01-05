package org.kettle.beam.transform;

import org.junit.Test;
import org.kettle.beam.util.BeamTransMetaUtil;
import org.pentaho.di.trans.TransMeta;

public class SwitchCasePipelineTest extends PipelineTestBase {

  @Test
  public void testSwitchCasePipeline() throws Exception {

    TransMeta transMeta = BeamTransMetaUtil.generateSwitchCaseTransMeta(
      "io-switch-case-output",
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