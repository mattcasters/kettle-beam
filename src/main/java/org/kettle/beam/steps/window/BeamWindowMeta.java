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
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaDate;
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
import org.pentaho.di.trans.steps.dummytrans.DummyTrans;
import org.pentaho.di.trans.steps.dummytrans.DummyTransData;
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
  public static final String MAX_WINDOW_FIELD = "max_window_field";
  public static final String START_WINDOW_FIELD = "start_window_field";
  public static final String END_WINDOW_FIELD = "end_window_field";

  private String windowType;
  private String duration;
  private String every;

  private String maxWindowField;
  private String startWindowField;
  private String endWindowField;

  public BeamWindowMeta() {
    super();
  }

  @Override public void setDefault() {
    windowType = BeamDefaults.WINDOW_TYPE_FIXED;
    duration = "60";
    every = "";
    startWindowField = "startWindow";
    endWindowField = "endWindow";
    maxWindowField = "maxWindow";
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new DummyTrans( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new DummyTransData();
  }

  @Override public String getDialogClassName() {
    return BeamWindowDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {

    if ( StringUtils.isNotEmpty( startWindowField )) {
      ValueMetaDate valueMeta = new ValueMetaDate( space.environmentSubstitute(startWindowField) );
      valueMeta.setOrigin( name );
      valueMeta.setConversionMask( ValueMetaBase.DEFAULT_DATE_FORMAT_MASK );
      inputRowMeta.addValueMeta( valueMeta );
    }
    if ( StringUtils.isNotEmpty( endWindowField )) {
      ValueMetaDate valueMeta = new ValueMetaDate( space.environmentSubstitute(endWindowField) );
      valueMeta.setOrigin( name );
      valueMeta.setConversionMask( ValueMetaBase.DEFAULT_DATE_FORMAT_MASK );
      inputRowMeta.addValueMeta( valueMeta );
    }
    if ( StringUtils.isNotEmpty( maxWindowField )) {
      ValueMetaDate valueMeta = new ValueMetaDate( space.environmentSubstitute(maxWindowField) );
      valueMeta.setOrigin( name );
      valueMeta.setConversionMask( ValueMetaBase.DEFAULT_DATE_FORMAT_MASK );
      inputRowMeta.addValueMeta( valueMeta );
    }

  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( WINDOW_TYPE, windowType ) );
    xml.append( XMLHandler.addTagValue( DURATION, duration ) );
    xml.append( XMLHandler.addTagValue( EVERY, every) );
    xml.append( XMLHandler.addTagValue( MAX_WINDOW_FIELD, maxWindowField) );
    xml.append( XMLHandler.addTagValue( START_WINDOW_FIELD, startWindowField) );
    xml.append( XMLHandler.addTagValue( END_WINDOW_FIELD, endWindowField) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    windowType = XMLHandler.getTagValue( stepnode, WINDOW_TYPE );
    duration = XMLHandler.getTagValue( stepnode, DURATION );
    every = XMLHandler.getTagValue( stepnode, EVERY);
    maxWindowField = XMLHandler.getTagValue( stepnode, MAX_WINDOW_FIELD);
    startWindowField = XMLHandler.getTagValue( stepnode, START_WINDOW_FIELD);
    endWindowField = XMLHandler.getTagValue( stepnode, END_WINDOW_FIELD);
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

  /**
   * Gets maxWindowField
   *
   * @return value of maxWindowField
   */
  public String getMaxWindowField() {
    return maxWindowField;
  }

  /**
   * @param maxWindowField The maxWindowField to set
   */
  public void setMaxWindowField( String maxWindowField ) {
    this.maxWindowField = maxWindowField;
  }

  /**
   * Gets startWindowField
   *
   * @return value of startWindowField
   */
  public String getStartWindowField() {
    return startWindowField;
  }

  /**
   * @param startWindowField The startWindowField to set
   */
  public void setStartWindowField( String startWindowField ) {
    this.startWindowField = startWindowField;
  }

  /**
   * Gets endWindowField
   *
   * @return value of endWindowField
   */
  public String getEndWindowField() {
    return endWindowField;
  }

  /**
   * @param endWindowField The endWindowField to set
   */
  public void setEndWindowField( String endWindowField ) {
    this.endWindowField = endWindowField;
  }
}
