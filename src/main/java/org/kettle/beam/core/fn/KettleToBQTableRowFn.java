package org.kettle.beam.core.fn;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class KettleToBQTableRowFn implements SerializableFunction<KettleRow, TableRow> {

  private String counterName;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private transient RowMetaInterface rowMeta;
  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;

  private transient SimpleDateFormat simpleDateFormat;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( KettleToBQTableRowFn.class );

  public KettleToBQTableRowFn( String counterName, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.counterName = counterName;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public TableRow apply( KettleRow inputRow ) {

    try {
      if ( rowMeta == null ) {
        readCounter = Metrics.counter( "read", counterName );
        outputCounter = Metrics.counter( "output", counterName );
        errorCounter = Metrics.counter( "error", counterName );

        // Initialize Kettle Beam
        //
        BeamKettle.init( stepPluginClasses, xpPluginClasses );
        rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        simpleDateFormat = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
        Metrics.counter( "init", counterName ).inc();
      }

      readCounter.inc();

      TableRow tableRow = new TableRow();
      for (int i=0;i<rowMeta.size();i++) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
        Object valueData = inputRow.getRow()[i];
        if (!valueMeta.isNull( valueData )) {
          switch ( valueMeta.getType() ) {
            case ValueMetaInterface.TYPE_STRING: tableRow.put( valueMeta.getName(), valueMeta.getString( valueData ) ); break;
            case ValueMetaInterface.TYPE_INTEGER: tableRow.put( valueMeta.getName(), valueMeta.getInteger( valueData ) ); break;
            case ValueMetaInterface.TYPE_DATE:
              Date date = valueMeta.getDate( valueData );
              String formattedDate = simpleDateFormat.format( date );
              tableRow.put( valueMeta.getName(), formattedDate);
              break;
            case ValueMetaInterface.TYPE_BOOLEAN: tableRow.put( valueMeta.getName(), valueMeta.getBoolean( valueData ) ); break;
            case ValueMetaInterface.TYPE_NUMBER: tableRow.put( valueMeta.getName(), valueMeta.getNumber( valueData ) ); break;
            default:
              throw new RuntimeException( "Data type conversion from Kettle to BigQuery TableRow not supported yet: " +valueMeta.toString());
          }
        }
      }

      // Pass the row to the process context
      //
      outputCounter.inc();

      return tableRow;

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.info( "Conversion error KettleRow to BigQuery TableRow : " + e.getMessage() );
      throw new RuntimeException( "Error converting KettleRow to BigQuery TableRow", e );
    }
  }


}
