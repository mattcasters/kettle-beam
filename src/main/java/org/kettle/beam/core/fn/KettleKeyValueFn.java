package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

// Split a Kettle row into key and values parts
//
public class KettleKeyValueFn extends DoFn<KettleRow, KV<KettleRow, KettleRow>> {

  private String inputRowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;
  private String[] keyFields;
  private String[] valueFields;
  private String counterName;

  private static final Logger LOG = LoggerFactory.getLogger( KettleKeyValueFn.class );

  private transient RowMetaInterface inputRowMeta;
  private transient int[] keyIndexes;
  private transient int[] valueIndexes;

  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter errorCounter;

  public KettleKeyValueFn() {
  }

  public KettleKeyValueFn( String inputRowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses,
                           String[] keyFields, String[] valueFields, String counterName) {
    this.inputRowMetaJson = inputRowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.keyFields = keyFields;
    this.valueFields = valueFields;
    this.counterName = counterName;
  }

  @Setup
  public void setUp() {
    try {
      readCounter = Metrics.counter( "read", counterName );
      errorCounter = Metrics.counter( "error", counterName );

      // Initialize Kettle Beam
      //
      BeamKettle.init(stepPluginClasses, xpPluginClasses);
      inputRowMeta = JsonRowMeta.fromJson( inputRowMetaJson );

      // Calculate key indexes
      //
      if ( keyFields.length==0) {
        throw new KettleException( "There are no group fields" );
      }
      keyIndexes = new int[ keyFields.length];
      for ( int i = 0; i< keyFields.length; i++) {
        keyIndexes[i]=inputRowMeta.indexOfValue( keyFields[i] );
        if ( keyIndexes[i]<0) {
          throw new KettleException( "Unable to find group by field '"+ keyFields[i]+"' in input "+inputRowMeta.toString() );
        }
      }

      // Calculate the value indexes
      //
      valueIndexes =new int[ valueFields.length];
      for ( int i = 0; i< valueFields.length; i++) {
        valueIndexes[i] = inputRowMeta.indexOfValue( valueFields[i] );
        if ( valueIndexes[i]<0) {
          throw new KettleException( "Unable to find subject by field '"+ valueFields[i]+"' in input "+inputRowMeta.toString() );
        }
      }

      // Now that we know everything, we can split the row...
      //
      Metrics.counter( "init", counterName ).inc();
    } catch(Exception e) {
      errorCounter.inc();
      LOG.error("Error setup of splitting row into key and value", e);
      throw new RuntimeException( "Unable to setup of split row into key and value", e );
    }
  }


  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      // Get an input row
      //
      KettleRow inputKettleRow = processContext.element();
      readCounter.inc();

      Object[] inputRow = inputKettleRow.getRow();

      // Copy over the data...
      //
      Object[] keyRow = RowDataUtil.allocateRowData( keyIndexes.length );
      for ( int i = 0; i< keyIndexes.length; i++) {
        keyRow[i] = inputRow[ keyIndexes[i]];
      }

      // Copy over the values...
      //
      Object[] valueRow = RowDataUtil.allocateRowData( valueIndexes.length );
      for ( int i = 0; i< valueIndexes.length; i++) {
        valueRow[i] = inputRow[ valueIndexes[i]];
      }

      KV<KettleRow, KettleRow> keyValue = KV.of( new KettleRow(keyRow), new KettleRow( valueRow ) );
      processContext.output( keyValue );

    } catch(Exception e) {
      errorCounter.inc();
      LOG.error("Error splitting row into key and value", e);
      throw new RuntimeException( "Unable to split row into key and value", e );
    }
  }


  /**
   * Gets inputRowMetaJson
   *
   * @return value of inputRowMetaJson
   */
  public String getInputRowMetaJson() {
    return inputRowMetaJson;
  }

  /**
   * @param inputRowMetaJson The inputRowMetaJson to set
   */
  public void setInputRowMetaJson( String inputRowMetaJson ) {
    this.inputRowMetaJson = inputRowMetaJson;
  }

  /**
   * Gets keyFields
   *
   * @return value of keyFields
   */
  public String[] getKeyFields() {
    return keyFields;
  }

  /**
   * @param keyFields The keyFields to set
   */
  public void setKeyFields( String[] keyFields ) {
    this.keyFields = keyFields;
  }

  /**
   * Gets valueFields
   *
   * @return value of valueFields
   */
  public String[] getValueFields() {
    return valueFields;
  }

  /**
   * @param valueFields The valueFields to set
   */
  public void setValueFields( String[] valueFields ) {
    this.valueFields = valueFields;
  }


}
