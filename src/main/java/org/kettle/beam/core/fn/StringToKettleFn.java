package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StringToKettleFn extends DoFn<String, KettleRow> {

  private String stepname;
  private String rowMetaJson;
  private String separator;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private transient Counter inputCounter;
  private transient Counter writtenCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( StringToKettleFn.class );

  private transient RowMetaInterface rowMeta;

  public StringToKettleFn( String stepname, String rowMetaJson, String separator, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.stepname = stepname;
    this.rowMetaJson = rowMetaJson;
    this.separator = separator;
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
      Metrics.counter( "error", stepname ).inc();
      LOG.error( "Error in setup of converting input data into Kettle rows : " + e.getMessage() );
      throw new RuntimeException( "Error in setup of converting input data into Kettle rows", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      String inputString = processContext.element();
      inputCounter.inc();

      String[] components = inputString.split( separator, -1 );

      // TODO: implement enclosure in FileDefinition
      //

      Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
      int index = 0;
      while ( index < rowMeta.size() && index < components.length ) {
        String sourceString = components[ index ];
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( index );
        ValueMetaInterface stringMeta = new ValueMetaString( "SourceString" );
        stringMeta.setConversionMask( valueMeta.getConversionMask() );
        try {
          row[ index ] = valueMeta.convertDataFromString( sourceString, stringMeta, null, null, ValueMetaInterface.TRIM_TYPE_NONE );
        } catch ( KettleValueException ve ) {
          throw new KettleException( "Unable to convert value '" + sourceString + "' to value : " + valueMeta.toStringMeta(), ve );
        }
        index++;
      }

      // Pass the row to the process context
      //
      processContext.output( new KettleRow( row ) );
      writtenCounter.inc();

    } catch ( Exception e ) {
      Metrics.counter( "error", stepname ).inc();
      LOG.error( "Error converting input data into Kettle rows " + processContext.element() + ", " + e.getMessage() );
      throw new RuntimeException( "Error converting input data into Kettle rows", e );

    }
  }


}
