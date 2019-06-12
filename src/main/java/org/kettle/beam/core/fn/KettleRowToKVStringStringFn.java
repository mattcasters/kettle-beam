package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KettleRowToKVStringStringFn extends DoFn<KettleRow, KV<String,String>> {

  private String rowMetaJson;
  private String stepname;
  private int keyIndex;
  private int valueIndex;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private static final Logger LOG = LoggerFactory.getLogger( KettleRowToKVStringStringFn.class );
  private final Counter numErrors = Metrics.counter( "main", "BeamSubscribeTransformErrors" );

  private RowMetaInterface rowMeta;
  private transient Counter initCounter;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;

  public KettleRowToKVStringStringFn( String stepname, int keyIndex, int valueIndex, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.stepname = stepname;
    this.keyIndex = keyIndex;
    this.valueIndex = valueIndex;
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
      LOG.error( "Error in setup of KettleRow to KV<String,String> function", e );
      throw new RuntimeException( "Error in setup of KettleRow to KV<String,String> function", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {
    try {
      KettleRow kettleRow = processContext.element();
      inputCounter.inc();

      String key = rowMeta.getString(kettleRow.getRow(), keyIndex);
      String value = rowMeta.getString(kettleRow.getRow(), valueIndex);

      processContext.output( KV.of( key, value ) );
      writtenCounter.inc();

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in KettleRow to KV<String,String> function", e );
      throw new RuntimeException( "Error in KettleRow to KV<String,String> function", e );
    }
  }
}