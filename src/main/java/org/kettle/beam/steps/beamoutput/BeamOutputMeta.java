package org.kettle.beam.steps.beamoutput;

import org.apache.commons.lang.StringUtils;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.w3c.dom.Node;

import java.util.List;

@Step(
  id = "BeamOutput",
  name = "Beam Output",
  description = "Describes a Beam Output",
  image = "beam-output.svg",
  categoryDescription = "Beam"
)
public class BeamOutputMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String OUTPUT_LOCATION = "output_location";
  public static final String FILE_DESCRIPTION_NAME = "file_description_name";
  public static final String FILE_PREFIX = "file_prefix";
  public static final String WINDOWED = "windowed";


  private String outputLocation;

  private String fileDescriptionName;

  private String filePrefix;

  private boolean windowed;

  @Override public void setDefault() {
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {

    return new BeamOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new BeamOutputData();
  }

  @Override public String getDialogClassName() {
    return BeamOutputDialog.class.getName();
  }

  public FileDefinition loadFileDefinition( IMetaStore metaStore) throws KettleStepException {
    if ( StringUtils.isEmpty(fileDescriptionName)) {
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

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer(  );

    xml.append( XMLHandler.addTagValue( OUTPUT_LOCATION, outputLocation ) );
    xml.append( XMLHandler.addTagValue( FILE_DESCRIPTION_NAME, fileDescriptionName) );
    xml.append( XMLHandler.addTagValue( FILE_PREFIX, filePrefix) );
    xml.append( XMLHandler.addTagValue( WINDOWED, windowed) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {

    outputLocation = XMLHandler.getTagValue( stepnode, OUTPUT_LOCATION );
    fileDescriptionName = XMLHandler.getTagValue( stepnode, FILE_DESCRIPTION_NAME );
    filePrefix = XMLHandler.getTagValue( stepnode, FILE_PREFIX );
    windowed = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, WINDOWED) );

  }

  /**
   * Gets outputLocation
   *
   * @return value of outputLocation
   */
  public String getOutputLocation() {
    return outputLocation;
  }

  /**
   * @param outputLocation The outputLocation to set
   */
  public void setOutputLocation( String outputLocation ) {
    this.outputLocation = outputLocation;
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

  /**
   * Gets filePrefix
   *
   * @return value of filePrefix
   */
  public String getFilePrefix() {
    return filePrefix;
  }

  /**
   * @param filePrefix The filePrefix to set
   */
  public void setFilePrefix( String filePrefix ) {
    this.filePrefix = filePrefix;
  }

  /**
   * Gets windowed
   *
   * @return value of windowed
   */
  public boolean isWindowed() {
    return windowed;
  }

  /**
   * @param windowed The windowed to set
   */
  public void setWindowed( boolean windowed ) {
    this.windowed = windowed;
  }
}
