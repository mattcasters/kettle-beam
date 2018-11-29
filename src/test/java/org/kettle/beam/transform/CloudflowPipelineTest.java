package org.kettle.beam.transform;

import junit.framework.TestCase;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.pipeline.TransMetaConverter;
import org.kettle.beam.util.BeamTransMetaUtil;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.io.File;

public class CloudflowPipelineTest extends TestCase {

  private IMetaStore metaStore;

  @Override protected void setUp() throws Exception {
    BeamKettle.init();

    metaStore = new MemoryMetaStore();

    System.setProperty( "GOOGLE_APPLICATION_CREDENTIALS", "/home/matt/parking/KettleDataflow-1947bf0f2032.json" );
  }

  @Test
  public void testCreatePipeline() throws Exception {

    TransMeta transMeta = BeamTransMetaUtil.generateBeamCloudDataFlowTransMeta(
      "First test transformation",
      "INPUT",
      "OUTPUT",
      metaStore
    );

    TransMetaConverter converter = new TransMetaConverter( transMeta, metaStore );
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    options.setProject( "kettledataflow" );
    options.setStagingLocation( "gs://kettledataflow/binaries" );
    options.setTempLocation("gs://kettledataflow/tmp/");
    Pipeline pipeline = converter.createPipeline( DataflowRunner.class, options );

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();

    MetricResults metricResults = pipelineResult.metrics();

    MetricQueryResults allResults = metricResults.queryMetrics( MetricsFilter.builder().build() );
    for ( MetricResult<Long> result : allResults.getCounters()) {
      System.out.println("Name: "+result.getName()+" Attempted: "+result.getAttempted()+ " Committed: "+result.getCommitted());
    }
  }
}
