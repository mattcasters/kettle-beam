package org.kettle.beam.core.transform;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.datastore.Key;
import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.KettleRowToEntityFn;
import org.kettle.beam.core.fn.KettleRowToKVStringStringFn;
import org.kettle.beam.core.fn.KettleToBQTableRowFn;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BeamFirestoreOutputTransform extends PTransform<PCollection<KettleRow>, PDone> {

    // These non-transient privates get serialized to spread across nodes
    //
    private String stepname;
    private String projectId;
    private String kind;
    private String keyField;
    private String jsonField;
    private String rowMetaJson;
    private List<String> stepPluginClasses;
    private List<String> xpPluginClasses;

    // Log and count errors.
    private static final Logger LOG = LoggerFactory.getLogger( BeamBQOutputTransform.class );
    private static final Counter numErrors = Metrics.counter( "main", "BeamFirestoreOutputError" );

    public BeamFirestoreOutputTransform() {
    }

    public BeamFirestoreOutputTransform( String stepname, String projectId, String kind, String keyField, String jsonField, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
        this.stepname = stepname;
        this.projectId = projectId;
        this.kind = kind;
        this.keyField = keyField;
        this.jsonField = jsonField;
        this.rowMetaJson = rowMetaJson;
        this.stepPluginClasses = stepPluginClasses;
        this.xpPluginClasses = xpPluginClasses;
    }

    @Override public PDone expand( PCollection<KettleRow> input ) {

        try {
            // Only initialize once on this node/vm
            //
            BeamKettle.init( stepPluginClasses, xpPluginClasses );

            // Inflate the metadata on the node where this is running...
            //
            RowMetaInterface rowMeta = JsonRowMeta.fromJson( rowMetaJson );


            // Datastore Writer
            //
            DatastoreV1.Write datastoreWrite = DatastoreIO.v1()
                    .write()
                    .withProjectId(this.projectId);

            KettleRowToEntityFn kettleRowToEntityFn = new KettleRowToEntityFn( stepname, this.projectId, this.kind, this.keyField, this.jsonField, rowMetaJson, stepPluginClasses, xpPluginClasses );
            PCollection<Entity> collection = input.apply(ParDo.of(kettleRowToEntityFn));

            return collection.apply(datastoreWrite);

        } catch ( Exception e ) {
            numErrors.inc();
            LOG.error( "Error in Beam Firestore Output transform", e );
            throw new RuntimeException( "Error in Beam Firestore Output transform", e );
        }
    }
}