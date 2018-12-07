package org.kettle.beam.transform;

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
import org.junit.Before;
import org.junit.Ignore;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.pipeline.TransMetaPipelineConverter;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.io.File;
import java.util.ArrayList;

public class PipelineTestBase {

  protected IMetaStore metaStore;

  @Before
  public void setUp() throws Exception {
    BeamKettle.init( new ArrayList<>(), new ArrayList<>() );

    metaStore = new MemoryMetaStore();

    File inputFolder = new File( "/tmp/customers/input" );
    inputFolder.mkdirs();
    File outputFolder = new File( "/tmp/customers/output" );
    outputFolder.mkdirs();
    File tmpFolder = new File( "/tmp/customers/tmp" );
    tmpFolder.mkdirs();

    FileUtils.copyFile( new File( "src/test/resources/customers/customers-100.txt" ), new File( "/tmp/customers/input/customers-100.txt" ) );
  }


  @Ignore
  public void createRunPipeline( TransMeta transMeta ) throws Exception {

    /*
    FileOutputStream fos = new FileOutputStream( "/tmp/"+transMeta.getName()+".ktr" );
    fos.write( transMeta.getXML().getBytes() );
    fos.close();
    */

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

    pipelineOptions.setJobName( transMeta.getName() );
    pipelineOptions.setUserAgent( BeamDefaults.STRING_KETTLE_BEAM );

    // No extra plugins to load : null option
    TransMetaPipelineConverter converter = new TransMetaPipelineConverter( transMeta, metaStore, (String) null );
    Pipeline pipeline = converter.createPipeline( DirectRunner.class, pipelineOptions );

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();

    MetricResults metricResults = pipelineResult.metrics();

    MetricQueryResults allResults = metricResults.queryMetrics( MetricsFilter.builder().build() );
    for ( MetricResult<Long> result : allResults.getCounters() ) {
      System.out.println( "Name: " + result.getName() + " Attempted: " + result.getAttempted() + " Committed: " + result.getCommitted() );
    }
  }
}