package org.kettle.beam.metastore;

import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

import java.util.ArrayList;
import java.util.List;

@MetaStoreElementType(
  name = "Kettle Beam Job Config",
  description = "Describes a Kettle Beam Job configuration"
)
public class BeamJobConfig {

  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private String runnerTypeName;

  //
  // Generic options
  //

  @MetaStoreAttribute
  private String userAgent;

  @MetaStoreAttribute
  private String tempLocation;

  @MetaStoreAttribute
  private String pluginsToStage;

  @MetaStoreAttribute
  private String initialNumberOfWorkers;

  @MetaStoreAttribute
  private String maximumNumberOfWokers;

  @MetaStoreAttribute
  private boolean streaming;

  @MetaStoreAttribute
  private String autoScalingAlgorithm;

  @MetaStoreAttribute
  private List<JobParameter> parameters;


  //
  // Dataflow specific options
  //

  @MetaStoreAttribute
  private String gcpProjectId;

  @MetaStoreAttribute
  private String gcpAppName;

  @MetaStoreAttribute
  private String gcpStagingLocation;

  @MetaStoreAttribute
  private String gcpWorkerMachineType;

  @MetaStoreAttribute
  private String gcpWorkerDiskType;

  @MetaStoreAttribute
  private String gcpDiskSizeGb;

  @MetaStoreAttribute
  private String gcpRegion;

  @MetaStoreAttribute
  private String gcpZone;



  public BeamJobConfig() {
    parameters = new ArrayList<>();
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets runnerTypeName
   *
   * @return value of runnerTypeName
   */
  public String getRunnerTypeName() {
    return runnerTypeName;
  }

  /**
   * @param runnerTypeName The runnerTypeName to set
   */
  public void setRunnerTypeName( String runnerTypeName ) {
    this.runnerTypeName = runnerTypeName;
  }

  /**
   * Gets userAgent
   *
   * @return value of userAgent
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * @param userAgent The userAgent to set
   */
  public void setUserAgent( String userAgent ) {
    this.userAgent = userAgent;
  }

  /**
   * Gets tempLocation
   *
   * @return value of tempLocation
   */
  public String getTempLocation() {
    return tempLocation;
  }

  /**
   * @param tempLocation The tempLocation to set
   */
  public void setTempLocation( String tempLocation ) {
    this.tempLocation = tempLocation;
  }

  /**
   * Gets gcpProjectId
   *
   * @return value of gcpProjectId
   */
  public String getGcpProjectId() {
    return gcpProjectId;
  }

  /**
   * @param gcpProjectId The gcpProjectId to set
   */
  public void setGcpProjectId( String gcpProjectId ) {
    this.gcpProjectId = gcpProjectId;
  }

  /**
   * Gets gcpAppName
   *
   * @return value of gcpAppName
   */
  public String getGcpAppName() {
    return gcpAppName;
  }

  /**
   * @param gcpAppName The gcpAppName to set
   */
  public void setGcpAppName( String gcpAppName ) {
    this.gcpAppName = gcpAppName;
  }

  /**
   * Gets gcpStagingLocation
   *
   * @return value of gcpStagingLocation
   */
  public String getGcpStagingLocation() {
    return gcpStagingLocation;
  }

  /**
   * @param gcpStagingLocation The gcpStagingLocation to set
   */
  public void setGcpStagingLocation( String gcpStagingLocation ) {
    this.gcpStagingLocation = gcpStagingLocation;
  }

  /**
   * Gets parameters
   *
   * @return value of parameters
   */
  public List<JobParameter> getParameters() {
    return parameters;
  }

  /**
   * @param parameters The parameters to set
   */
  public void setParameters( List<JobParameter> parameters ) {
    this.parameters = parameters;
  }

  /**
   * Gets pluginsToStage
   *
   * @return value of pluginsToStage
   */
  public String getPluginsToStage() {
    return pluginsToStage;
  }

  /**
   * @param pluginsToStage The pluginsToStage to set
   */
  public void setPluginsToStage( String pluginsToStage ) {
    this.pluginsToStage = pluginsToStage;
  }

  /**
   * Gets initialNumberOfWorkers
   *
   * @return value of initialNumberOfWorkers
   */
  public String getInitialNumberOfWorkers() {
    return initialNumberOfWorkers;
  }

  /**
   * @param initialNumberOfWorkers The initialNumberOfWorkers to set
   */
  public void setInitialNumberOfWorkers( String initialNumberOfWorkers ) {
    this.initialNumberOfWorkers = initialNumberOfWorkers;
  }

  /**
   * Gets maximumNumberOfWokers
   *
   * @return value of maximumNumberOfWokers
   */
  public String getMaximumNumberOfWokers() {
    return maximumNumberOfWokers;
  }

  /**
   * @param maximumNumberOfWokers The maximumNumberOfWokers to set
   */
  public void setMaximumNumberOfWokers( String maximumNumberOfWokers ) {
    this.maximumNumberOfWokers = maximumNumberOfWokers;
  }

  /**
   * Gets streaming
   *
   * @return value of streaming
   */
  public boolean isStreaming() {
    return streaming;
  }

  /**
   * @param streaming The streaming to set
   */
  public void setStreaming( boolean streaming ) {
    this.streaming = streaming;
  }

  /**
   * Gets autoScalingAlgorithm
   *
   * @return value of autoScalingAlgorithm
   */
  public String getAutoScalingAlgorithm() {
    return autoScalingAlgorithm;
  }

  /**
   * @param autoScalingAlgorithm The autoScalingAlgorithm to set
   */
  public void setAutoScalingAlgorithm( String autoScalingAlgorithm ) {
    this.autoScalingAlgorithm = autoScalingAlgorithm;
  }

  /**
   * Gets gcpWorkerMachineType
   *
   * @return value of gcpWorkerMachineType
   */
  public String getGcpWorkerMachineType() {
    return gcpWorkerMachineType;
  }

  /**
   * @param gcpWorkerMachineType The gcpWorkerMachineType to set
   */
  public void setGcpWorkerMachineType( String gcpWorkerMachineType ) {
    this.gcpWorkerMachineType = gcpWorkerMachineType;
  }

  /**
   * Gets gcpWorkerDiskType
   *
   * @return value of gcpWorkerDiskType
   */
  public String getGcpWorkerDiskType() {
    return gcpWorkerDiskType;
  }

  /**
   * @param gcpWorkerDiskType The gcpWorkerDiskType to set
   */
  public void setGcpWorkerDiskType( String gcpWorkerDiskType ) {
    this.gcpWorkerDiskType = gcpWorkerDiskType;
  }

  /**
   * Gets gcpDiskSizeGb
   *
   * @return value of gcpDiskSizeGb
   */
  public String getGcpDiskSizeGb() {
    return gcpDiskSizeGb;
  }

  /**
   * @param gcpDiskSizeGb The gcpDiskSizeGb to set
   */
  public void setGcpDiskSizeGb( String gcpDiskSizeGb ) {
    this.gcpDiskSizeGb = gcpDiskSizeGb;
  }

  /**
   * Gets gcpRegion
   *
   * @return value of gcpRegion
   */
  public String getGcpRegion() {
    return gcpRegion;
  }

  /**
   * @param gcpRegion The gcpRegion to set
   */
  public void setGcpRegion( String gcpRegion ) {
    this.gcpRegion = gcpRegion;
  }

  /**
   * Gets gcpZone
   *
   * @return value of gcpZone
   */
  public String getGcpZone() {
    return gcpZone;
  }

  /**
   * @param gcpZone The gcpZone to set
   */
  public void setGcpZone( String gcpZone ) {
    this.gcpZone = gcpZone;
  }
}
