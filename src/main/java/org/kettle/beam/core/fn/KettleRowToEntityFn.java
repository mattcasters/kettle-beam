package org.kettle.beam.core.fn;

import com.google.cloud.Timestamp;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.NullValue;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.Json;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.core.util.Strings;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class KettleRowToEntityFn extends DoFn<KettleRow, Entity> {

    private String rowMetaJson;
    private String stepname;
    private String projectId;
    private String kind;
    private String keyField;
    private String jsonField;
    private List<String> stepPluginClasses;
    private List<String> xpPluginClasses;
    private List<String> keyValues;

    private static final Logger LOG = LoggerFactory.getLogger( KettleRowToEntityFn.class );
    private final Counter numErrors = Metrics.counter( "main", "BeamFirestoreOutputErrors" );

    private RowMetaInterface rowMeta;
    private transient Counter initCounter;
    private transient Counter inputCounter;
    private transient Counter writtenCounter;

    public KettleRowToEntityFn( String stepname, String projectId, String kind, String keyField, String jsonField, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
        this.stepname = stepname;
        this.projectId = projectId;
        this.kind = kind;
        this.keyField = keyField;
        this.jsonField = jsonField;
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
            this.rowMeta = JsonRowMeta.fromJson( rowMetaJson );
            this.keyValues = new ArrayList<>();

            Metrics.counter( "init", stepname ).inc();
        } catch ( Exception e ) {
            numErrors.inc();
            LOG.error( "Error in setup of KettleRow to Entity function", e );
            throw new RuntimeException( "Error in setup of KettleRow to Entity function", e );
        }
    }

    @ProcessElement
    public void processElement( ProcessContext processContext ) {
        try {
            KettleRow kettleRow = processContext.element();
            inputCounter.inc();

            Map<String, Object> dataSet = this.getDateSet(kettleRow.getRow());

            Entity.Builder entityBuilder = Entity.newBuilder();

            Map<String, Object> fields = null;
            if(!Strings.isNullOrEmpty(this.jsonField)) {
                Object json = dataSet.get(jsonField);
                if (json != null) {
                    fields = Json.getInstance().deserialize(json.toString());
                }
            }else {
                fields = dataSet;
            }
            boolean isParsed = this.parse(fields, entityBuilder);


            if(isParsed) {
                processContext.output(entityBuilder.build());
            }
            writtenCounter.inc();

        } catch ( Exception e ) {
            numErrors.inc();
            LOG.error( "Error in KettleRow to Entity function -> step '" + this.stepname + "'", e );
            throw new RuntimeException( "Error in KettleRow to Entity function", e );
        }
    }

    private Map<String, Object> getDateSet(Object[] row) throws KettleValueException {
        Map<String, Object> dataSet = new HashMap<>();
        String field;
        Object value;
        int i = 0;
        for(ValueMetaInterface valueMetaInterface : this.rowMeta.getValueMetaList()){
            field = valueMetaInterface.getName().trim();
            value = null;
            switch (valueMetaInterface.getType()){
                case ValueMetaInterface.TYPE_STRING: value = this.rowMeta.getString(row, i); break;
                case ValueMetaInterface.TYPE_INTEGER: value = this.rowMeta.getInteger(row, i); break;
                case ValueMetaInterface.TYPE_NUMBER: value = this.rowMeta.getNumber(row, i); break;
                case ValueMetaInterface.TYPE_BIGNUMBER: value = this.rowMeta.getBigNumber(row, i); break;
                case ValueMetaInterface.TYPE_BOOLEAN: value = this.rowMeta.getBoolean(row, i); break;
                case ValueMetaInterface.TYPE_DATE: value = this.rowMeta.getDate(row, i); break;
                case ValueMetaInterface.TYPE_TIMESTAMP: value = this.rowMeta.getDate(row, i); break;
                case ValueMetaInterface.TYPE_INET: value = this.rowMeta.getString(row, i); break;
                case ValueMetaInterface.TYPE_NONE: value = this.rowMeta.getString(row, i); break;
                case ValueMetaInterface.TYPE_SERIALIZABLE: value = this.rowMeta.getString(row, i); break;
            }
            dataSet.put(field, value);
            i++;
        }
        return dataSet;
    }

    private boolean parse(Map<String, Object> fields, Entity.Builder entityBuilder) throws Exception{
        Boolean result = false;
        String field;
        Object value;
        Value entityValue = null;
        boolean isFoundKey = false;

        for(Map.Entry<String, Object> entry : fields.entrySet()){
            field = entry.getKey();
            value = entry.getValue();

            if (value == null) {
                entityValue = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
                result = true;

            } else if (value instanceof Integer || value instanceof Short) {
                entityValue = Value.newBuilder().setIntegerValue(new Integer(value.toString())).build();
                result = true;

            } else if (value instanceof Double) {
                entityValue = Value.newBuilder().setDoubleValue((Double)value).build();
                result = true;

            } else if (value instanceof BigDecimal) {
                entityValue = Value.newBuilder().setDoubleValue(((BigDecimal)value).doubleValue()).build();
                result = true;

            } else if (value instanceof Boolean) {
                entityValue = Value.newBuilder().setBooleanValue((Boolean)value).build();
                result = true;

            } else if (value instanceof com.google.protobuf.Timestamp) {
                entityValue = Value.newBuilder().setTimestampValue((com.google.protobuf.Timestamp)value).build();
                result = true;

            } else if (value instanceof Date) {
                entityValue = Value.newBuilder().setTimestampValue(com.google.protobuf.util.Timestamps.fromMillis(((Date) value).getTime())).build();
                result = true;

            }else {
                entityValue = Value.newBuilder().setStringValue(value.toString()).build();
                result = true;

            }

            entityBuilder.putProperties(field, entityValue);
            if(field.equalsIgnoreCase(this.keyField)){
                String keyValue = value.toString();
                if(this.keyValues.contains(keyValue)){return false;}
                Key key = DatastoreHelper.makeKey(this.kind, value.toString()).build();
                entityBuilder.setKey(key);
                this.keyValues.add(keyValue);
                isFoundKey = true;
            }

        }

        if(!isFoundKey){throw new Exception("Campo Chave '" + this.keyField + "' não encontrado na entidade.");}

        return result;
    }


}