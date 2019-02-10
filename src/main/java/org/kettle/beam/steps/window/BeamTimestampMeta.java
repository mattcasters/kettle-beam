package org.kettle.beam.steps.window;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaDate;
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

import java.util.List;

@Step(
  id = "BeamTimestamp",
  name = "Beam Timestamp",
  description = "Add timestamps to a bounded data source",
  image = "beam-timestamp.svg",
  categoryDescription = "Big Data"
)
public class BeamTimestampMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String FIELD_NAME = "field_name";
  public static final String READ_TIMESTAMP = "read_timestamp";

  private String fieldName;

  private boolean readingTimestamp;

  public BeamTimestampMeta() {
    super();
  }

  @Override public void setDefault() {
    fieldName = "";
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new DummyTrans( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new DummyTransData();
  }

  @Override public String getDialogClassName() {
    return BeamTimestampDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {

    if ( readingTimestamp ) {
      ValueMetaDate valueMeta = new ValueMetaDate( fieldName );
      valueMeta.setOrigin( name );
      inputRowMeta.addValueMeta( valueMeta );
    }
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( FIELD_NAME, fieldName ) );
    xml.append( XMLHandler.addTagValue( READ_TIMESTAMP, readingTimestamp ) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    fieldName = XMLHandler.getTagValue( stepnode, FIELD_NAME );
    readingTimestamp = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, READ_TIMESTAMP ) );
  }


  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set
   */
  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * Gets readingTimestamp
   *
   * @return value of readingTimestamp
   */
  public boolean isReadingTimestamp() {
    return readingTimestamp;
  }

  /**
   * @param readingTimestamp The readingTimestamp to set
   */
  public void setReadingTimestamp( boolean readingTimestamp ) {
    this.readingTimestamp = readingTimestamp;
  }
}
