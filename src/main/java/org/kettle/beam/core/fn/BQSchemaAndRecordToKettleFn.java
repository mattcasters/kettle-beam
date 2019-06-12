package org.kettle.beam.core.fn;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.List;

/**
 * BigQuery Avro SchemaRecord to KettleRow
 */
public class BQSchemaAndRecordToKettleFn implements SerializableFunction<SchemaAndRecord, KettleRow> {

  private String stepname;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private transient Counter initCounter;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( BQSchemaAndRecordToKettleFn.class );

  private transient RowMetaInterface rowMeta;
  private transient SimpleDateFormat simpleDateTimeFormat;
  private transient SimpleDateFormat simpleDateFormat;

  public BQSchemaAndRecordToKettleFn( String stepname, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.stepname = stepname;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  public KettleRow apply( SchemaAndRecord schemaAndRecord ) {

    try {

      GenericRecord record = schemaAndRecord.getRecord();
      TableSchema tableSchema = schemaAndRecord.getTableSchema();

      if ( rowMeta == null ) {

        inputCounter = Metrics.counter( "input", stepname );
        writtenCounter = Metrics.counter( "written", stepname );
        errorCounter = Metrics.counter( "error", stepname );

        // Initialize Kettle
        //
        BeamKettle.init( stepPluginClasses, xpPluginClasses );
        rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        int[] valueTypes = new int[rowMeta.size()];

        List<TableFieldSchema> fields = tableSchema.getFields();
        for (int i=0;i<fields.size();i++) {
          TableFieldSchema fieldSchema = fields.get( i );
          String name = fieldSchema.getName();
          int index = rowMeta.indexOfValue( name );
          // Ignore everything we didn't ask for.
          //
          if (index>=0) {
            String avroTypeString = fieldSchema.getType();
            try {
              AvroType avroType = AvroType.valueOf( avroTypeString );
              valueTypes[index] = avroType.getKettleType();
            } catch(IllegalArgumentException e) {
              throw new RuntimeException( "Unable to recognize data type '"+avroTypeString+"'", e );
            }
          }
        }

        // See that we got all the fields covered...
        //
        for (int i = 0;i<rowMeta.size();i++) {
          if (valueTypes[i]==0) {
            ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
            throw new RuntimeException( "Unable to find field '"+valueMeta.getName()+"'" );
          }
        }

        simpleDateTimeFormat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss" );
        simpleDateTimeFormat.setLenient( true );
        simpleDateFormat = new SimpleDateFormat( "yyyy-MM-dd" );
        simpleDateFormat.setLenient( true );
        Metrics.counter( "init", stepname ).inc();
      }

      inputCounter.inc();

      // Convert to the requested Kettle Data types
      //
      Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
      for (int index=0; index < rowMeta.size() ; index++) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( index );
        Object srcData = record.get(valueMeta.getName());
        if (srcData!=null) {
          switch(valueMeta.getType()) {
            case ValueMetaInterface.TYPE_STRING:
              row[index] = srcData.toString();
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              row[index] = (Long)srcData;
              break;
            case ValueMetaInterface.TYPE_NUMBER:
              row[index] = (Double)srcData;
              break;
            case ValueMetaInterface.TYPE_BOOLEAN:
              row[index] = (Boolean)srcData;
              break;
            case ValueMetaInterface.TYPE_DATE:
              // We get a Long back
              //
              String datetimeString = ((Utf8) srcData).toString();
              if (datetimeString.length()==10) {
                row[index] = simpleDateFormat.parse( datetimeString );
              } else {
                row[ index ] = simpleDateTimeFormat.parse( datetimeString );
              }
              break;
            default:
              throw new RuntimeException("Conversion from Avro JSON to Kettle is not yet supported for Kettle data type '"+valueMeta.getTypeDesc()+"'");
          }
        }
      }

      // Pass the row to the process context
      //
      writtenCounter.inc();
      return new KettleRow( row );

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.error( "Error converting BQ Avro data into Kettle rows : " + e.getMessage() );
      throw new RuntimeException( "Error converting BQ Avro data into Kettle rows", e );

    }
  }

  //  From:
  //         https://cloud.google.com/dataprep/docs/html/BigQuery-Data-Type-Conversions_102563896
  //
  public enum AvroType {
    STRING(ValueMetaInterface.TYPE_STRING),
    BYTES(ValueMetaInterface.TYPE_STRING),
    INTEGER(ValueMetaInterface.TYPE_INTEGER),
    INT64(ValueMetaInterface.TYPE_INTEGER),
    FLOAT(ValueMetaInterface.TYPE_NUMBER),
    FLOAT64(ValueMetaInterface.TYPE_NUMBER),
    BOOLEAN(ValueMetaInterface.TYPE_BOOLEAN),
    BOOL(ValueMetaInterface.TYPE_BOOLEAN),
    TIMESTAMP(ValueMetaInterface.TYPE_DATE),
    DATE(ValueMetaInterface.TYPE_DATE),
    TIME(ValueMetaInterface.TYPE_DATE),
    DATETIME(ValueMetaInterface.TYPE_DATE),
    ;

    private int kettleType;

    private AvroType(int kettleType) {
      this.kettleType = kettleType;
    }

    /**
     * Gets kettleType
     *
     * @return value of kettleType
     */
    public int getKettleType() {
      return kettleType;
    }
  }

}
