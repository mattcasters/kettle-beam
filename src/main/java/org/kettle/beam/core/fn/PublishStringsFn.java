package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PublishStringsFn extends DoFn<KettleRow, String> {

  private String rowMetaJson;
  private int fieldIndex;
  private String stepname;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private static final Logger LOG = LoggerFactory.getLogger( PublishStringsFn.class );
  private final Counter numErrors = Metrics.counter( "main", "BeamPublishTransformErrors" );

  private RowMetaInterface rowMeta;
  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter outputCounter;

  public PublishStringsFn( String stepname, int fieldIndex, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.stepname = stepname;
    this.fieldIndex = fieldIndex;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      readCounter = Metrics.counter( "read", stepname );
      outputCounter = Metrics.counter( "output", stepname );

      // Initialize Kettle Beam
      //
      BeamKettle.init( stepPluginClasses, xpPluginClasses );
      rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( "init", stepname ).inc();
    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in setup of pub/sub publish messages function", e );
      throw new RuntimeException( "Error in setup of pub/sub publish messages function", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      KettleRow kettleRow = processContext.element();
      readCounter.inc();

      try {
        String string = rowMeta.getString( kettleRow.getRow(), fieldIndex );
        processContext.output( string );
        outputCounter.inc();
      } catch ( Exception e ) {
        throw new RuntimeException( "Unable to pass string", e );
      }

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in pub/sub publish messages function", e );
      throw new RuntimeException( "Error in pub/sub publish messages function", e );
    }
  }
}