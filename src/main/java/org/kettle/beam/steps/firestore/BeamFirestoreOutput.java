package org.kettle.beam.steps.firestore;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import org.kettle.beam.core.util.Json;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.userdefinedjavaclass.TransformClassBase;

import java.math.BigDecimal;
import java.util.*;

/**
 * Classe responsável por execução do step
 * Firestore Output.
 *
 * @author Renato Dornelas Cardoso <renato@romaconsulting.com.br>
 */
public class BeamFirestoreOutput extends BaseStep implements StepInterface {

    private Datastore datastore;

    /**
     * This is the base step that forms that basis for all steps. You can derive from this class to implement your own
     * steps.
     *
     * @param stepMeta          The StepMeta object to run.
     * @param stepDataInterface the data object to store temporary data, database connections, caches, result sets,
     *                          hashtables etc.
     * @param copyNr            The copynumber for this step.
     * @param transMeta         The TransInfo of which the step stepMeta is part of.
     * @param trans             The (running) transformation to obtain information shared among the steps.
     */
    public BeamFirestoreOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    }

    private Datastore getDatastore(){
        if(this.datastore == null) {
            this.datastore = DatastoreOptions.getDefaultInstance().getService();
        }
        return this.datastore;
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
        try {
            Object[] row = this.getRow();
            if (row == null) {
                this.setOutputDone();
                return false;
            }

            BeamFirestoreOutputMeta meta = (BeamFirestoreOutputMeta) smi;
            BeamFirestoreOutputData data = (BeamFirestoreOutputData) sdi;

            if(Strings.isNullOrEmpty(meta.getKind())){throw new Exception("Kind não informado.");}
            if(Strings.isNullOrEmpty(meta.getKeyField())){throw new Exception("Campo chave não informado.");}

            String kind = meta.getKind().trim();
            String keyField = meta.getKeyField().trim();
            String jsonField = !Strings.isNullOrEmpty(meta.getJsonField()) ? meta.getJsonField().trim() : "";


            Map<String, Object> dataSet = this.getDateSet(row);
            Map<String, Object> fields = null;
            if (!Strings.isNullOrEmpty(jsonField)) {
                Object json = dataSet.get(jsonField);
                if (json != null) {
                    fields = Json.getInstance().deserialize(json.toString());
                }
            } else {
                fields = dataSet;
            }


            if(fields != null) {
                if (!fields.containsKey(keyField)) {
                    this.log.logError("Campo chave '" + keyField + "' não encontrado.");
                    this.setOutputDone();
                    return false;
                }
                Object value = fields.get(keyField);
                if (value == null) {
                    this.log.logError("Valor da chave '" + keyField + "' não pode ser nulo.");
                    this.setOutputDone();
                    return false;
                }

                Entity entity;
                Entity.Builder entityBuilder;
                Key newKey = this.getDatastore().newKeyFactory().setKind(kind).newKey(value.toString());
                entityBuilder = Entity.newBuilder(newKey);

                boolean isParsed = this.parse(fields, entityBuilder);

                if (isParsed) {
                    entity = entityBuilder.build();
                    this.getDatastore().put(entity);
                }
            }

            return true;

        }catch (KettleException ex){
            throw ex;

        }catch (Exception ex){
            this.log.logError("Firestore Output Error", ex);
            return false;
        }
    }

    private Map<String, Object> getDateSet(Object[] row) throws KettleValueException {
        Map<String, Object> dataSet = new HashMap<>();
        String field;
        Object value;
        int i = 0;
        for(ValueMetaInterface valueMetaInterface : this.getInputRowMeta().getValueMetaList()){
            field = valueMetaInterface.getName().trim();
            value = null;
            switch (valueMetaInterface.getType()){
                case ValueMetaInterface.TYPE_STRING: value = this.getInputRowMeta().getString(row, i); break;
                case ValueMetaInterface.TYPE_INTEGER: value = this.getInputRowMeta().getInteger(row, i); break;
                case ValueMetaInterface.TYPE_NUMBER: value = this.getInputRowMeta().getNumber(row, i); break;
                case ValueMetaInterface.TYPE_BIGNUMBER: value = this.getInputRowMeta().getBigNumber(row, i); break;
                case ValueMetaInterface.TYPE_BOOLEAN: value = this.getInputRowMeta().getBoolean(row, i); break;
                case ValueMetaInterface.TYPE_DATE: value = this.getInputRowMeta().getDate(row, i); break;
                case ValueMetaInterface.TYPE_TIMESTAMP: value = this.getInputRowMeta().getDate(row, i); break;
                case ValueMetaInterface.TYPE_INET: value = this.getInputRowMeta().getString(row, i); break;
                case ValueMetaInterface.TYPE_NONE: value = this.getInputRowMeta().getString(row, i); break;
                case ValueMetaInterface.TYPE_SERIALIZABLE: value = this.getInputRowMeta().getString(row, i); break;
            }
            dataSet.put(field, value);
            i++;
        }
        return dataSet;
    }

    private boolean parse(Map<String, Object> data, Entity.Builder entityBuilder){
        Boolean result = false;
        String field;
        Object value;

        for(Map.Entry<String, Object> entry : data.entrySet()){
            field = entry.getKey();
            value = entry.getValue();

            if(value == null){
                entityBuilder.setNull(field);
                result = true;

            }else if (value instanceof String) {
                entityBuilder.set(field, (String) value);
                result = true;

            } else if (value instanceof Long || value instanceof Integer || value instanceof Short) {
                entityBuilder.set(field, new Long(value.toString()));
                result = true;

            } else if (value instanceof Double) {
                entityBuilder.set(field, (Double) value);
                result = true;

            } else if (value instanceof BigDecimal) {
                entityBuilder.set(field, ((BigDecimal) value).doubleValue());
                result = true;

            } else if (value instanceof Boolean) {
                entityBuilder.set(field, (Boolean) value);
                result = true;

            } else if (value instanceof Timestamp) {
                entityBuilder.set(field, (Timestamp) value);
                result = true;

            } else if (value instanceof Date) {
                entityBuilder.set(field, Timestamp.of((Date) value));
                result = true;
            }

        }
        return result;
    }

}

