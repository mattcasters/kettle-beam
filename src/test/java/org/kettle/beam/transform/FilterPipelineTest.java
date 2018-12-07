package org.kettle.beam.transform;

import junit.framework.TestCase;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.pipeline.TransMetaPipelineConverter;
import org.kettle.beam.util.BeamTransMetaUtil;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.io.File;

public class FilterPipelineTest extends PipelineTestBase {

  @Test
  public void testFilterRowsPipeline() throws Exception {

    TransMeta transMeta = BeamTransMetaUtil.generateFilterRowsTransMeta(
      "input-filter-rows-output",
      "INPUT",
      "OUTPUT",
      metaStore
    );

    createRunPipeline( transMeta );
  }

}