package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KVLongStringToKettleRowFn extends DoFn<KV<Long,String>, KettleRow> {

  private String rowMetaJson;
  private String stepname;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private static final Logger LOG = LoggerFactory.getLogger( KVLongStringToKettleRowFn.class );
  private final Counter numErrors = Metrics.counter( "main", "BeamSubscribeTransformErrors" );

  private RowMetaInterface rowMeta;
  private transient Counter initCounter;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;

  public KVLongStringToKettleRowFn( String stepname, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.stepname = stepname;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      inputCounter = Metrics.counter( "input", stepname );
      writtenCounter = Metrics.counter( "written", stepname );

      // Initialize Kettle Beam
      //
      BeamKettle.init( stepPluginClasses, xpPluginClasses );
      rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( "init", stepname ).inc();
    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in setup of KV<Long,String> to Kettle Row conversion function", e );
      throw new RuntimeException( "Error in setup of KV<Long,String> to Kettle Row conversion function", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {
    try {

      KV<Long,String> kv = processContext.element();
      inputCounter.inc();

      Object[] outputRow = RowDataUtil.allocateRowData( rowMeta.size() );
      outputRow[ 0 ] = kv.getKey();
      outputRow[ 1 ] = kv.getValue();

      processContext.output( new KettleRow( outputRow ) );
      writtenCounter.inc();

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in KV<Long,String> to Kettle Row conversion function", e );
      throw new RuntimeException( "Error in KV<Long,String> to Kettle Row conversion function", e );
    }
  }
}