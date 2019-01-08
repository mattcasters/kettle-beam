package org.kettle.beam.steps.window;

import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamDefaults;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaSerializable;
import org.pentaho.di.core.row.value.ValueMetaString;
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
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@Step(
  id = "BeamWindow",
  name = "Beam Window",
  description = "Create a Beam Window",
  image = "beam-window.svg",
  categoryDescription = "Big Data"
)
public class BeamWindowMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String WINDOW_TYPE = "window_type";
  public static final String DURATION = "duration";
  public static final String EVERY = "every";

  private String windowType;
  private String duration;
  private String every;

  public BeamWindowMeta() {
    super();
  }

  @Override public void setDefault() {
    windowType = BeamDefaults.WINDOW_TYPE_FIXED;
    duration = "60";
    every = "";
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new BeamWindow( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new BeamWindowData();
  }

  @Override public String getDialogClassName() {
    return BeamWindowDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {

    // No change to the stream
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( WINDOW_TYPE, windowType ) );
    xml.append( XMLHandler.addTagValue( DURATION, duration ) );
    xml.append( XMLHandler.addTagValue( EVERY, every) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    windowType = XMLHandler.getTagValue( stepnode, WINDOW_TYPE );
    duration = XMLHandler.getTagValue( stepnode, DURATION );
    every = XMLHandler.getTagValue( stepnode, EVERY);
  }


  /**
   * Gets windowType
   *
   * @return value of windowType
   */
  public String getWindowType() {
    return windowType;
  }

  /**
   * @param windowType The windowType to set
   */
  public void setWindowType( String windowType ) {
    this.windowType = windowType;
  }

  /**
   * Gets duration
   *
   * @return value of duration
   */
  public String getDuration() {
    return duration;
  }

  /**
   * @param duration The duration to set
   */
  public void setDuration( String duration ) {
    this.duration = duration;
  }

  /**
   * Gets every
   *
   * @return value of every
   */
  public String getEvery() {
    return every;
  }

  /**
   * @param every The every to set
   */
  public void setEvery( String every ) {
    this.every = every;
  }
}
