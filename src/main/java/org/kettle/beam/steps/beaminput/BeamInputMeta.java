package org.kettle.beam.steps.beaminput;

import org.apache.commons.lang.StringUtils;
import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

@Step(
  id = "BeamOutput",
  name = "Beam Input",
  description = "Describes a Beam Input",
  image = "beam-input.svg",
  categoryDescription = "Beam"
)
public class BeamInputMeta extends BaseStepMeta implements StepMetaInterface {

  private String inputLocation;

  private String fileDescriptionName;

  @Override public void setDefault() {
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new BeamInput( stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  @Override public StepDataInterface getStepData() {
    return new BeamInputData();
  }

  @Override public String getDialogClassName() {
    return BeamInputDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {

    FileDefinition fileDefinition = loadFileDefinition(metaStore);

    try {
      inputRowMeta.clear();
      inputRowMeta.addRowMeta( fileDefinition.getRowMeta() );
    } catch( KettlePluginException e ) {
      throw new KettleStepException( "Unable to get row layout of file definition '"+fileDefinition.getName()+"'", e );
    }
  }

  public FileDefinition loadFileDefinition(IMetaStore metaStore) throws KettleStepException {
    if (StringUtils.isEmpty(fileDescriptionName)) {
      throw new KettleStepException("No file description name provided");
    }
    FileDefinition fileDefinition;
    try {
      MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, metaStore, PentahoDefaults.NAMESPACE );
      fileDefinition = factory.loadElement( fileDescriptionName );
    } catch(Exception e) {
      throw new KettleStepException( "Unable to load file description '"+fileDescriptionName+"' from the metastore", e );
    }

    return fileDefinition;
  }

  // TODO: READ/WRITE XML

  /**
   * Gets inputLocation
   *
   * @return value of inputLocation
   */
  public String getInputLocation() {
    return inputLocation;
  }

  /**
   * @param inputLocation The inputLocation to set
   */
  public void setInputLocation( String inputLocation ) {
    this.inputLocation = inputLocation;
  }

  /**
   * Gets fileDescriptionName
   *
   * @return value of fileDescriptionName
   */
  public String getFileDescriptionName() {
    return fileDescriptionName;
  }

  /**
   * @param fileDescriptionName The fileDescriptionName to set
   */
  public void setFileDescriptionName( String fileDescriptionName ) {
    this.fileDescriptionName = fileDescriptionName;
  }

}
