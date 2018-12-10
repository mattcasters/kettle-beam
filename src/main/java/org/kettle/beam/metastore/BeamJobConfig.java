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
  private String gcpInitialNumberOfWorkers;

  @MetaStoreAttribute
  private String gcpMaximumNumberOfWokers;

  @MetaStoreAttribute
  private String gcpAutoScalingAlgorithm;

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

  @MetaStoreAttribute
  private boolean gcpStreaming;


  //
  // Spark specific options
  //


  @MetaStoreAttribute
  private String sparkMaster;

  @MetaStoreAttribute
  private String sparkBatchIntervalMillis;

  @MetaStoreAttribute
  private String sparkCheckpointDir;

  @MetaStoreAttribute
  private String sparkCheckpointDurationMillis;

  @MetaStoreAttribute
  private boolean sparkEnableSparkMetricSinks;

  @MetaStoreAttribute
  private String sparkMaxRecordsPerBatch;

  @MetaStoreAttribute
  private String sparkMinReadTimeMillis;

  @MetaStoreAttribute
  private String sparkReadTimePercentage;

  @MetaStoreAttribute
  private String sparkBundleSize;

  @MetaStoreAttribute
  private String sparkStorageLevel;


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
   * Gets gcpInitialNumberOfWorkers
   *
   * @return value of gcpInitialNumberOfWorkers
   */
  public String getGcpInitialNumberOfWorkers() {
    return gcpInitialNumberOfWorkers;
  }

  /**
   * @param gcpInitialNumberOfWorkers The gcpInitialNumberOfWorkers to set
   */
  public void setGcpInitialNumberOfWorkers( String gcpInitialNumberOfWorkers ) {
    this.gcpInitialNumberOfWorkers = gcpInitialNumberOfWorkers;
  }

  /**
   * Gets gcpMaximumNumberOfWokers
   *
   * @return value of gcpMaximumNumberOfWokers
   */
  public String getGcpMaximumNumberOfWokers() {
    return gcpMaximumNumberOfWokers;
  }

  /**
   * @param gcpMaximumNumberOfWokers The gcpMaximumNumberOfWokers to set
   */
  public void setGcpMaximumNumberOfWokers( String gcpMaximumNumberOfWokers ) {
    this.gcpMaximumNumberOfWokers = gcpMaximumNumberOfWokers;
  }

  /**
   * Gets gcpStreaming
   *
   * @return value of gcpStreaming
   */
  public boolean isGcpStreaming() {
    return gcpStreaming;
  }

  /**
   * @param gcpStreaming The gcpStreaming to set
   */
  public void setGcpStreaming( boolean gcpStreaming ) {
    this.gcpStreaming = gcpStreaming;
  }

  /**
   * Gets gcpAutoScalingAlgorithm
   *
   * @return value of gcpAutoScalingAlgorithm
   */
  public String getGcpAutoScalingAlgorithm() {
    return gcpAutoScalingAlgorithm;
  }

  /**
   * @param gcpAutoScalingAlgorithm The gcpAutoScalingAlgorithm to set
   */
  public void setGcpAutoScalingAlgorithm( String gcpAutoScalingAlgorithm ) {
    this.gcpAutoScalingAlgorithm = gcpAutoScalingAlgorithm;
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

  /**
   * Gets sparkBatchIntervalMillis
   *
   * @return value of sparkBatchIntervalMillis
   */
  public String getSparkBatchIntervalMillis() {
    return sparkBatchIntervalMillis;
  }

  /**
   * @param sparkBatchIntervalMillis The sparkBatchIntervalMillis to set
   */
  public void setSparkBatchIntervalMillis( String sparkBatchIntervalMillis ) {
    this.sparkBatchIntervalMillis = sparkBatchIntervalMillis;
  }

  /**
   * Gets sparkCheckpointDir
   *
   * @return value of sparkCheckpointDir
   */
  public String getSparkCheckpointDir() {
    return sparkCheckpointDir;
  }

  /**
   * @param sparkCheckpointDir The sparkCheckpointDir to set
   */
  public void setSparkCheckpointDir( String sparkCheckpointDir ) {
    this.sparkCheckpointDir = sparkCheckpointDir;
  }

  /**
   * Gets sparkCheckpointDurationMillis
   *
   * @return value of sparkCheckpointDurationMillis
   */
  public String getSparkCheckpointDurationMillis() {
    return sparkCheckpointDurationMillis;
  }

  /**
   * @param sparkCheckpointDurationMillis The sparkCheckpointDurationMillis to set
   */
  public void setSparkCheckpointDurationMillis( String sparkCheckpointDurationMillis ) {
    this.sparkCheckpointDurationMillis = sparkCheckpointDurationMillis;
  }

  /**
   * Gets sparkEnableSparkMetricSinks
   *
   * @return value of sparkEnableSparkMetricSinks
   */
  public boolean isSparkEnableSparkMetricSinks() {
    return sparkEnableSparkMetricSinks;
  }

  /**
   * @param sparkEnableSparkMetricSinks The sparkEnableSparkMetricSinks to set
   */
  public void setSparkEnableSparkMetricSinks( boolean sparkEnableSparkMetricSinks ) {
    this.sparkEnableSparkMetricSinks = sparkEnableSparkMetricSinks;
  }

  /**
   * Gets sparkMaxRecordsPerBatch
   *
   * @return value of sparkMaxRecordsPerBatch
   */
  public String getSparkMaxRecordsPerBatch() {
    return sparkMaxRecordsPerBatch;
  }

  /**
   * @param sparkMaxRecordsPerBatch The sparkMaxRecordsPerBatch to set
   */
  public void setSparkMaxRecordsPerBatch( String sparkMaxRecordsPerBatch ) {
    this.sparkMaxRecordsPerBatch = sparkMaxRecordsPerBatch;
  }

  /**
   * Gets sparkMinReadTimeMillis
   *
   * @return value of sparkMinReadTimeMillis
   */
  public String getSparkMinReadTimeMillis() {
    return sparkMinReadTimeMillis;
  }

  /**
   * @param sparkMinReadTimeMillis The sparkMinReadTimeMillis to set
   */
  public void setSparkMinReadTimeMillis( String sparkMinReadTimeMillis ) {
    this.sparkMinReadTimeMillis = sparkMinReadTimeMillis;
  }

  /**
   * Gets sparkReadTimePercentage
   *
   * @return value of sparkReadTimePercentage
   */
  public String getSparkReadTimePercentage() {
    return sparkReadTimePercentage;
  }

  /**
   * @param sparkReadTimePercentage The sparkReadTimePercentage to set
   */
  public void setSparkReadTimePercentage( String sparkReadTimePercentage ) {
    this.sparkReadTimePercentage = sparkReadTimePercentage;
  }

  /**
   * Gets sparkBundleSize
   *
   * @return value of sparkBundleSize
   */
  public String getSparkBundleSize() {
    return sparkBundleSize;
  }

  /**
   * @param sparkBundleSize The sparkBundleSize to set
   */
  public void setSparkBundleSize( String sparkBundleSize ) {
    this.sparkBundleSize = sparkBundleSize;
  }

  /**
   * Gets sparkMaster
   *
   * @return value of sparkMaster
   */
  public String getSparkMaster() {
    return sparkMaster;
  }

  /**
   * @param sparkMaster The sparkMaster to set
   */
  public void setSparkMaster( String sparkMaster ) {
    this.sparkMaster = sparkMaster;
  }

  /**
   * Gets sparkStorageLevel
   *
   * @return value of sparkStorageLevel
   */
  public String getSparkStorageLevel() {
    return sparkStorageLevel;
  }

  /**
   * @param sparkStorageLevel The sparkStorageLevel to set
   */
  public void setSparkStorageLevel( String sparkStorageLevel ) {
    this.sparkStorageLevel = sparkStorageLevel;
  }
}
