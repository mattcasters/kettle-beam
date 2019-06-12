package org.kettle.beam.core.transform;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.BQSchemaAndRecordToKettleFn;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

public class BeamBQInputTransform extends PTransform<PBegin, PCollection<KettleRow>> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepname;
  private String projectId;
  private String datasetId;
  private String tableId;
  private String query;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamBQInputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamBQInputError" );

  private transient RowMetaInterface rowMeta;

  public BeamBQInputTransform() {
  }

  public BeamBQInputTransform( @Nullable String name, String stepname, String projectId, String datasetId, String tableId, String query, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( name );
    this.stepname = stepname;
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.tableId = tableId;
    this.query = query;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PCollection<KettleRow> expand( PBegin input ) {
    try {
      // Only initialize once on this node/vm
      //
      BeamKettle.init(stepPluginClasses, xpPluginClasses);

      // Function to convert from Avro to Kettle rows
      //
      BQSchemaAndRecordToKettleFn toKettleFn = new BQSchemaAndRecordToKettleFn( stepname, rowMetaJson, stepPluginClasses, xpPluginClasses );

      TableReference tableReference = new TableReference();
      if (StringUtils.isNotEmpty( projectId )) {
        tableReference.setProjectId( projectId );
      }
      tableReference.setDatasetId( datasetId );
      tableReference.setTableId( tableId );

      BigQueryIO.TypedRead<KettleRow> bqTypedRead;

      if (StringUtils.isEmpty( query )) {
        bqTypedRead = BigQueryIO
          .read( toKettleFn )
          .from( tableReference )
        ;
      } else {
        bqTypedRead = BigQueryIO
          .read( toKettleFn )
          .fromQuery( query )
        ;
      }

      // Apply the function
      //
      PCollection<KettleRow> output = input.apply( bqTypedRead );

      return output;

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in beam input transform", e );
      throw new RuntimeException( "Error in beam input transform", e );
    }
  }
}
