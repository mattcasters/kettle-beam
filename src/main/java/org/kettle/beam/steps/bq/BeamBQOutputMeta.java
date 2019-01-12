package org.kettle.beam.steps.bq;

import org.apache.commons.lang.StringUtils;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
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
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step(
  id = "BeamBQOutput",
  name = "Beam BigQuery Output",
  description = "Writes to a BigQuery table in Beam",
  image = "beam-bq-output.svg",
  categoryDescription = "Big Data"
)
public class BeamBQOutputMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String PROJECT_ID = "project_id";
  public static final String DATASET_ID = "dataset_id";
  public static final String TABLE_ID = "table_id";

  private String projectId;
  private String datasetId;
  private String tableId;

  @Override public void setDefault() {
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {

    // This is an endpoint in Beam, produces no further output
    //
    inputRowMeta.clear();
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new DummyTrans( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new DummyTransData();
  }

  @Override public String getDialogClassName() {
    return BeamBQOutputDialog.class.getName();
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( PROJECT_ID, projectId ) );
    xml.append( XMLHandler.addTagValue( DATASET_ID, datasetId ) );
    xml.append( XMLHandler.addTagValue( TABLE_ID, tableId) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    projectId = XMLHandler.getTagValue( stepNode, PROJECT_ID );
    datasetId= XMLHandler.getTagValue( stepNode, DATASET_ID );
    tableId= XMLHandler.getTagValue( stepNode, TABLE_ID);
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
}
