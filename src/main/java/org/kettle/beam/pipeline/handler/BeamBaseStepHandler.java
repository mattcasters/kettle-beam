package org.kettle.beam.pipeline.handler;

import org.kettle.beam.metastore.BeamJobConfig;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;

public class BeamBaseStepHandler {

  protected IMetaStore metaStore;
  protected TransMeta transMeta;
  protected List<String> stepPluginClasses;
  protected List<String> xpPluginClasses;
  protected boolean input;
  protected boolean output;
  protected BeamJobConfig beamJobConfig;

  public BeamBaseStepHandler( BeamJobConfig beamJobConfig, boolean input, boolean output, IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.beamJobConfig = beamJobConfig;
    this.input = input;
    this.output = output;
    this.metaStore = metaStore;
    this.transMeta = transMeta;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  /**
   * Gets metaStore
   *
   * @return value of metaStore
   */
  public IMetaStore getMetaStore() {
    return metaStore;
  }

  /**
   * @param metaStore The metaStore to set
   */
  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  /**
   * Gets transMeta
   *
   * @return value of transMeta
   */
  public TransMeta getTransMeta() {
    return transMeta;
  }

  /**
   * @param transMeta The transMeta to set
   */
  public void setTransMeta( TransMeta transMeta ) {
    this.transMeta = transMeta;
  }

  /**
   * Gets stepPluginClasses
   *
   * @return value of stepPluginClasses
   */
  public List<String> getStepPluginClasses() {
    return stepPluginClasses;
  }

  /**
   * @param stepPluginClasses The stepPluginClasses to set
   */
  public void setStepPluginClasses( List<String> stepPluginClasses ) {
    this.stepPluginClasses = stepPluginClasses;
  }

  /**
   * Gets xpPluginClasses
   *
   * @return value of xpPluginClasses
   */
  public List<String> getXpPluginClasses() {
    return xpPluginClasses;
  }

  /**
   * @param xpPluginClasses The xpPluginClasses to set
   */
  public void setXpPluginClasses( List<String> xpPluginClasses ) {
    this.xpPluginClasses = xpPluginClasses;
  }

  /**
   * Gets input
   *
   * @return value of input
   */
  public boolean isInput() {
    return input;
  }

  /**
   * @param input The input to set
   */
  public void setInput( boolean input ) {
    this.input = input;
  }

  /**
   * Gets output
   *
   * @return value of output
   */
  public boolean isOutput() {
    return output;
  }

  /**
   * @param output The output to set
   */
  public void setOutput( boolean output ) {
    this.output = output;
  }
}
