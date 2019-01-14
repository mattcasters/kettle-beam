package org.kettle.beam.steps.bq;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.dummytrans.DummyTrans;
import org.pentaho.di.trans.steps.dummytrans.DummyTransData;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step(
  id = "BeamBQInput",
  name = "Beam BigQuery Input",
  description = "Reads from a BigQuery table in Beam",
  image = "beam-bq-input.svg",
  categoryDescription = "Big Data"
)
public class BeamBQInputMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String PROJECT_ID = "project_id";
  public static final String DATASET_ID = "dataset_id";
  public static final String TABLE_ID = "table_id";
  public static final String QUERY = "query";

  private String projectId;
  private String datasetId;
  private String tableId;
  private String query;

  private List<BQField> fields;

  public BeamBQInputMeta() {
    super();
    fields = new ArrayList<>();
  }

  @Override public void setDefault() {
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new DummyTrans( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new DummyTransData();
  }

  @Override public String getDialogClassName() {
    return BeamBQInputDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {

    try {
      for ( BQField field : fields ) {
        int type = ValueMetaFactory.getIdForValueMeta( field.getKettleType() );
        ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta( field.getNewNameOrName(), type, -1, -1 );
        valueMeta.setOrigin( name );
        inputRowMeta.addValueMeta( valueMeta );
      }
    } catch ( Exception e ) {
      throw new KettleStepException( "Error getting Beam BQ Input step output", e );
    }
  }


  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();

    xml.append( XMLHandler.addTagValue( PROJECT_ID, projectId ) );
    xml.append( XMLHandler.addTagValue( DATASET_ID, datasetId ) );
    xml.append( XMLHandler.addTagValue( TABLE_ID, tableId ) );
    xml.append( XMLHandler.addTagValue( QUERY, query ) );

    xml.append( XMLHandler.openTag( "fields" ) );
    for ( BQField field : fields ) {
      xml.append( XMLHandler.openTag( "field" ) );
      xml.append( XMLHandler.addTagValue( "name", field.getName() ) );
      xml.append( XMLHandler.addTagValue( "new_name", field.getNewName() ) );
      xml.append( XMLHandler.addTagValue( "type", field.getKettleType() ) );
      xml.append( XMLHandler.closeTag( "field" ) );
    }
    xml.append( XMLHandler.closeTag( "fields" ) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {

    projectId = XMLHandler.getTagValue( stepNode, PROJECT_ID );
    datasetId = XMLHandler.getTagValue( stepNode, DATASET_ID );
    tableId = XMLHandler.getTagValue( stepNode, TABLE_ID );
    query = XMLHandler.getTagValue( stepNode, QUERY );

    Node fieldsNode = XMLHandler.getSubNode( stepNode, "fields" );
    List<Node> fieldNodes = XMLHandler.getNodes( fieldsNode, "field" );
    fields = new ArrayList<>();
    for ( Node fieldNode : fieldNodes ) {
      String name = XMLHandler.getTagValue( fieldNode, "name" );
      String newName = XMLHandler.getTagValue( fieldNode, "new_name" );
      String kettleType = XMLHandler.getTagValue( fieldNode, "type" );
      fields.add( new BQField( name, newName, kettleType ) );
    }
  }

  /**
   * Gets projectId
   *
   * @return value of projectId
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * @param projectId The projectId to set
   */
  public void setProjectId( String projectId ) {
    this.projectId = projectId;
  }

  /**
   * Gets datasetId
   *
   * @return value of datasetId
   */
  public String getDatasetId() {
    return datasetId;
  }

  /**
   * @param datasetId The datasetId to set
   */
  public void setDatasetId( String datasetId ) {
    this.datasetId = datasetId;
  }

  /**
   * Gets tableId
   *
   * @return value of tableId
   */
  public String getTableId() {
    return tableId;
  }

  /**
   * @param tableId The tableId to set
   */
  public void setTableId( String tableId ) {
    this.tableId = tableId;
  }

  /**
   * Gets query
   *
   * @return value of query
   */
  public String getQuery() {
    return query;
  }

  /**
   * @param query The query to set
   */
  public void setQuery( String query ) {
    this.query = query;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<BQField> getFields() {
    return fields;
  }

  /**
   * @param fields The fields to set
   */
  public void setFields( List<BQField> fields ) {
    this.fields = fields;
  }
}
