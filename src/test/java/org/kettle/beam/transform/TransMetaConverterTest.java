package org.kettle.beam.transform;

import junit.framework.TestCase;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.pipeline.TransMetaConverter;
import org.kettle.beam.util.BeamTransMetaUtil;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.io.File;

public class TransMetaConverterTest extends TestCase {

  private IMetaStore metaStore;

  @Override protected void setUp() throws Exception {
    BeamKettle.init();

    metaStore = new MemoryMetaStore();

    File inputFolder = new File("/tmp/customers/input");
    inputFolder.mkdirs();
    File outputFolder = new File("/tmp/customers/output");
    outputFolder.mkdirs();
    File tmpFolder = new File("/tmp/customers/tmp");
    tmpFolder.mkdirs();

    FileUtils.copyFile(new File("src/test/resources/customers/customers-100.txt"), new File("/tmp/customers/input/customers-100.txt"));
  }

  @Test
  public void testCreatePipeline() throws Exception {

    TransMeta transMeta = BeamTransMetaUtil.generateBeamInputOutputTransMeta(
      "First test transformation",
      "INPUT",
      "OUTPUT",
      metaStore
    );

    TransMetaConverter converter = new TransMetaConverter( transMeta, metaStore );
    Pipeline pipeline = converter.createPipeline();

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();

    MetricResults metricResults = pipelineResult.metrics();

    MetricQueryResults allResults = metricResults.queryMetrics( MetricsFilter.builder().build() );
    for ( MetricResult<Long> result : allResults.getCounters()) {
      System.out.println("Name: "+result.getName()+" Attempted: "+result.getAttempted()+ " Committed: "+result.getCommitted());
    }
  }
}
