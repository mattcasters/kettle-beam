package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KettleToStringFn extends DoFn<KettleRow, String> {

  private String counterName;
  private String outputLocation;
  private String separator;
  private String enclosure;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private transient RowMetaInterface rowMeta;
  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( KettleToStringFn.class );

  public KettleToStringFn( String counterName, String outputLocation, String separator, String enclosure, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.counterName = counterName;
    this.outputLocation = outputLocation;
    this.separator = separator;
    this.enclosure = enclosure;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      readCounter = Metrics.counter( "read", counterName );
      outputCounter = Metrics.counter( "output", counterName );
      errorCounter = Metrics.counter( "error", counterName );

      // Initialize Kettle Beam
      //
      BeamKettle.init( stepPluginClasses, xpPluginClasses );
      rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( "init", counterName ).inc();
    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.info( "Parse error on setup of Kettle data to string lines : " + e.getMessage() );
      throw new RuntimeException( "Error on setup of converting Kettle data to string lines", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      KettleRow inputRow = processContext.element();
      readCounter.inc();

      // Just a quick and dirty output for now...
      // TODO: refine with multiple output formats, Avro, Parquet, ...
      //
      StringBuffer line = new StringBuffer();

      for ( int i = 0; i < rowMeta.size(); i++ ) {

        if ( i > 0 ) {
          line.append( separator );
        }

        String valueString = rowMeta.getString( inputRow.getRow(), i );

        if ( valueString != null ) {
          boolean enclose = false;
          if ( StringUtils.isNotEmpty( enclosure ) ) {
            enclose = valueString.contains( enclosure );
          }
          if ( enclose ) {
            line.append( enclosure );
          }
          line.append( valueString );
          if ( enclose ) {
            line.append( enclosure );
          }
        }
      }

      // Pass the row to the process context
      //
      processContext.output( line.toString() );
      outputCounter.inc();

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.info( "Parse error on " + processContext.element() + ", " + e.getMessage() );
      throw new RuntimeException( "Error converting Kettle data to string lines", e );
    }
  }


}
