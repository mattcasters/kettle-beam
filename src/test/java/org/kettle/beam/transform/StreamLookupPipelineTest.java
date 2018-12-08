package org.kettle.beam.transform;

import org.junit.Test;
import org.kettle.beam.util.BeamTransMetaUtil;
import org.pentaho.di.trans.TransMeta;

public class StreamLookupPipelineTest extends PipelineTestBase {

  @Test
  public void testStreamLookupPipeline() throws Exception {

    TransMeta transMeta = BeamTransMetaUtil.generateStreamLookupTransMeta(
      "input-stream-lookup-output",
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