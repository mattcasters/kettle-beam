package org.kettle.beam.pipeline;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.kettle.beam.core.metastore.SerializableMetaStore;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.metastore.JobParameter;
import org.kettle.beam.metastore.RunnerType;
import org.kettle.beam.pipeline.fatjar.FatJarBuilder;
import org.kettle.beam.pipeline.spark.MainSpark;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import static org.kettle.beam.pipeline.TransMetaPipelineConverter.findAnnotatedClasses;

public class KettleBeamPipelineExecutor {

  private LogChannelInterface logChannel;
  private TransMeta transMeta;
  private BeamJobConfig jobConfig;
  private IMetaStore metaStore;
  private ClassLoader classLoader;
  private List<BeamMetricsUpdatedListener> updatedListeners;
  private boolean loggingMetrics;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;
  private JavaSparkContext sparkContext;

  private KettleBeamPipelineExecutor() {
    this.updatedListeners = new ArrayList<>();
  }

  public KettleBeamPipelineExecutor( LogChannelInterface log, TransMeta transMeta, BeamJobConfig jobConfig, IMetaStore metaStore, ClassLoader classLoader ) {
    this(log, transMeta, jobConfig, metaStore, classLoader, null, null);
  }

  public KettleBeamPipelineExecutor( LogChannelInterface log, TransMeta transMeta, BeamJobConfig jobConfig, IMetaStore metaStore, ClassLoader classLoader, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this();
    this.logChannel = log;
    this.transMeta = transMeta;
    this.jobConfig = jobConfig;
    this.metaStore = metaStore;
    this.classLoader = classLoader;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.loggingMetrics = true;
  }

  public PipelineResult execute() throws KettleException {
    return execute( false );
  }

  public PipelineResult execute( boolean server ) throws KettleException {

    if (StringUtils.isEmpty(jobConfig.getRunnerTypeName())) {
      throw new KettleException( "Please specify a runner type" );
    }
    RunnerType runnerType = RunnerType.getRunnerTypeByName( transMeta.environmentSubstitute( jobConfig.getRunnerTypeName() ) );
    if (runnerType==null) {
      throw new KettleException( "Runner type '"+jobConfig.getRunnerTypeName()+"' is not recognized");
    }
    switch ( runnerType ) {
      case Direct:
      case Flink:
      case DataFlow:
        return executePipeline();

      case Spark:
        if ( server ) {
          return executePipeline();
        } else {
          return executeSpark();
        }

      default:
        throw new KettleException( "Execution on runner '" + runnerType.name() + "' is not supported yet, sorry." );
    }
  }

  private PipelineResult executeSpark() throws KettleException {
    try {

      // Write our artifacts to the deploy folder of the spark environment
      //
      String deployFolder = transMeta.environmentSubstitute( jobConfig.getSparkDeployFolder() );
      if ( !deployFolder.endsWith( File.separator ) ) {
        deployFolder += File.separator;
      }

      // The transformation
      //
      String shortTransformationFilename = "transformation.ktr";
      String transformationFilename = deployFolder + shortTransformationFilename;
      FileUtils.copyFile( new File( transMeta.getFilename() ), new File( transformationFilename ) );

      // Serialize TransMeta and MetaStore, set as variables...
      //
      SerializableMetaStore serializableMetaStore = new SerializableMetaStore( metaStore );
      String shortMetastoreJsonFilename = "metastore.json"; ;
      String metastoreJsonFilename = deployFolder + shortMetastoreJsonFilename;
      FileUtils.writeStringToFile( new File( metastoreJsonFilename ), serializableMetaStore.toJson(), "UTF-8" );

      // Create a fat jar...
      //
      // Find the list of jar files to stage...
      //
      List<String> libraryFilesToStage;
      if (StringUtils.isNotEmpty(jobConfig.getFatJar())) {
        libraryFilesToStage = new ArrayList<>(  );
        libraryFilesToStage.add(jobConfig.getFatJar());
      } else {
        libraryFilesToStage = BeamConst.findLibraryFilesToStage( null, transMeta.environmentSubstitute( jobConfig.getPluginsToStage() ), true, true );
      }

      String shortFatJarFilename = "kettle-beam-fat.jar";
      String fatJarFilename = deployFolder + shortFatJarFilename;

      FatJarBuilder fatJarBuilder = new FatJarBuilder( fatJarFilename, libraryFilesToStage );
      // if (!new File(fatJarBuilder.getTargetJarFile()).exists()) {
      fatJarBuilder.buildTargetJar();
      // }

      String master = transMeta.environmentSubstitute( jobConfig.getSparkMaster() );

      // Figure out the list of step and XP plugin classes...
      //
      StringBuilder stepPluginClasses = new StringBuilder();
      StringBuilder xpPluginClasses = new StringBuilder();

      String pluginsToStage = jobConfig.getPluginsToStage();
      if (!pluginsToStage.contains( "kettle-beam" )) {
        if ( StringUtils.isEmpty( pluginsToStage ) ) {
          pluginsToStage = "kettle-beam";
        } else {
          pluginsToStage = "kettle-beam,"+pluginsToStage;
        }
      }

      if ( StringUtils.isNotEmpty( pluginsToStage ) ) {
        String[] pluginFolders = pluginsToStage.split( "," );
        for ( String pluginFolder : pluginFolders ) {
          List<String> stepClasses = findAnnotatedClasses( pluginFolder, Step.class.getName() );
          for (String stepClass : stepClasses) {
            if (stepPluginClasses.length()>0) {
              stepPluginClasses.append(",");
            }
            stepPluginClasses.append(stepClass);
          }
          List<String> xpClasses = findAnnotatedClasses( pluginFolder, ExtensionPoint.class.getName() );
          for (String xpClass : xpClasses) {
            if (xpPluginClasses.length()>0) {
              xpPluginClasses.append(",");
            }
            xpPluginClasses.append(xpClass);
          }
        }
      }

      // Write the spark-submit command
      //
      StringBuilder command = new StringBuilder();
      command.append( "spark-submit" ).append( " \\\n" );
      command.append( " --class " ).append( MainSpark.class.getName() ).append( " \\\n" );
      command.append( " --master " ).append( master ).append( " \\\n" );
      command.append( " --deploy-mode cluster" ).append( " \\\n" );
      command.append( " --files " ).append( shortTransformationFilename ).append( "," ).append( shortMetastoreJsonFilename ).append( " \\\n" );
      command.append( " " ).append( shortFatJarFilename ).append( " \\\n" );
      command.append( " " ).append( shortTransformationFilename ).append( " \\\n" );
      command.append( " " ).append( shortMetastoreJsonFilename ).append( " \\\n" );
      command.append( " '" ).append( jobConfig.getName() ).append( "'" ).append( " \\\n" );
      command.append( " '" ).append( master ).append( "'" ).append( " \\\n" );
      command.append( " '" ).append( transMeta.getName() ).append( "'" ).append(" \\\n");
      command.append( " " ).append( stepPluginClasses.toString() ).append(" \\\n");
      command.append( " " ).append( xpPluginClasses.toString() ).append(" \\\n");
      command.append( "\n" ).append( "\n" );

      // Write this command to file
      //
      String commandFilename = deployFolder + "submit-command.sh";
      FileUtils.writeStringToFile( new File( commandFilename ), command.toString() );

      // TODO, unify it all...
      //
      return null;
    } catch ( Exception e ) {
      throw new KettleException( "Error executing transformation on Spark", e );
    }
  }

  private PipelineResult executePipeline() throws KettleException {
    ClassLoader oldContextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Explain to various classes in the Beam API (@see org.apache.beam.sdk.io.FileSystems)
      // what the context classloader is.
      // Set it back when we're done here.
      //
      Thread.currentThread().setContextClassLoader( classLoader );

      final Pipeline pipeline = getPipeline( transMeta, jobConfig );

      logChannel.logBasic( "Creation of Apache Beam pipeline is complete. Starting execution..." );

      // This next command can block on certain runners...
      //
      PipelineResult pipelineResult = pipeline.run();

      Timer timer = new Timer();
      TimerTask timerTask = new TimerTask() {
        @Override public void run() {

          // Log the metrics...
          //
          if ( isLoggingMetrics() ) {
            logMetrics( pipelineResult );
          }

          // Update the listeners.
          //
          updateListeners( pipelineResult );
        }
      };
      // Every 5 seconds
      //
      timer.schedule( timerTask, 5000, 5000 );

      // Wait until we're done
      //
      pipelineResult.waitUntilFinish();

      timer.cancel();
      timer.purge();

      // Log the metrics at the end.
      logMetrics( pipelineResult );

      // Update a last time
      //
      updateListeners( pipelineResult );

      logChannel.logBasic( "  ----------------- End of Beam job " + pipeline.getOptions().getJobName() + " -----------------------" );

      return pipelineResult;
    } catch(Exception e) {
      throw new KettleException( "Error building/executing pipeline", e );
    } finally {
      Thread.currentThread().setContextClassLoader( oldContextClassLoader );
    }

  }


  private void logMetrics( PipelineResult pipelineResult ) {
    MetricResults metricResults = pipelineResult.metrics();

    logChannel.logBasic( "  ----------------- Metrics refresh @ " + new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss" ).format( new Date() ) + " -----------------------" );

    MetricQueryResults allResults = metricResults.queryMetrics( MetricsFilter.builder().build() );
    for ( MetricResult<Long> result : allResults.getCounters() ) {
      logChannel.logBasic( "Name: " + result.getName() + " Attempted: " + result.getAttempted() );
    }
  }

  public void updateListeners( PipelineResult pipelineResult ) {
    for ( BeamMetricsUpdatedListener listener : updatedListeners ) {
      listener.beamMetricsUpdated( pipelineResult );
    }
  }

  public Pipeline getPipeline( TransMeta transMeta, BeamJobConfig config ) throws KettleException {

    try {

      if ( StringUtils.isEmpty( config.getRunnerTypeName() ) ) {
        throw new KettleException( "You need to specify a runner type, one of : " + RunnerType.values().toString() );
      }
      PipelineOptions pipelineOptions = null;
      VariableSpace space = transMeta;

      RunnerType runnerType = RunnerType.getRunnerTypeByName( transMeta.environmentSubstitute( config.getRunnerTypeName() ) );
      switch ( runnerType ) {
        case Direct:
          pipelineOptions = PipelineOptionsFactory.create();
          break;
        case DataFlow:
          DataflowPipelineOptions dfOptions = PipelineOptionsFactory.as( DataflowPipelineOptions.class );
          configureDataFlowOptions( config, dfOptions, space );
          pipelineOptions = dfOptions;
          break;
        case Spark:
          SparkPipelineOptions sparkOptions;
          if (sparkContext!=null) {
            SparkContextOptions sparkContextOptions = PipelineOptionsFactory.as( SparkContextOptions.class );
            sparkContextOptions.setProvidedSparkContext( sparkContext );
            sparkOptions = sparkContextOptions;
          } else {
            sparkOptions = PipelineOptionsFactory.as( SparkPipelineOptions.class );
          }
          configureSparkOptions( config, sparkOptions, space, transMeta.getName() );
          pipelineOptions = sparkOptions;
          break;
        case Flink:
          FlinkPipelineOptions flinkOptions = PipelineOptionsFactory.as( FlinkPipelineOptions.class );
          configureFlinkOptions( config, flinkOptions, space );
          pipelineOptions = flinkOptions;
          break;
        default:
          throw new KettleException( "Sorry, this isn't implemented yet" );
      }

      configureStandardOptions( config, transMeta.getName(), pipelineOptions, space );

      setVariablesInTransformation( config, transMeta );

      TransMetaPipelineConverter converter;
      if (stepPluginClasses!=null && xpPluginClasses!=null) {
        converter = new TransMetaPipelineConverter( transMeta, metaStore, stepPluginClasses, xpPluginClasses, jobConfig );
      } else {
        converter = new TransMetaPipelineConverter( transMeta, metaStore, config.getPluginsToStage(), jobConfig );
      }
      Pipeline pipeline = converter.createPipeline( pipelineOptions );

      // Also set the pipeline options...
      //
      FileSystems.setDefaultPipelineOptions(pipelineOptions);

      return pipeline;
    } catch ( Exception e ) {
      throw new KettleException( "Error configuring local Beam Engine", e );
    }

  }

  private void configureStandardOptions( BeamJobConfig config, String transformationName, PipelineOptions pipelineOptions, VariableSpace space ) {
    if ( StringUtils.isNotEmpty( transformationName ) ) {
      String sanitizedName = transformationName.replaceAll( "[^-A-Za-z0-9]", "" )
        ;
      pipelineOptions.setJobName( sanitizedName );
    }
    if ( StringUtils.isNotEmpty( config.getUserAgent() ) ) {
      String userAgent = space.environmentSubstitute( config.getUserAgent() );
      pipelineOptions.setUserAgent( userAgent );
    }
    if ( StringUtils.isNotEmpty( config.getTempLocation() ) ) {
      String tempLocation = space.environmentSubstitute( config.getTempLocation() );
      pipelineOptions.setTempLocation( tempLocation );
    }
  }

  private void setVariablesInTransformation( BeamJobConfig config, TransMeta transMeta ) {
    String[] parameters = transMeta.listParameters();
    for ( JobParameter parameter : config.getParameters() ) {
      if ( StringUtils.isNotEmpty( parameter.getVariable() ) ) {
        if ( Const.indexOfString( parameter.getVariable(), parameters ) >= 0 ) {
          try {
            transMeta.setParameterValue( parameter.getVariable(), parameter.getValue() );
          } catch ( UnknownParamException e ) {
            transMeta.setVariable( parameter.getVariable(), parameter.getValue() );
          }
        } else {
          transMeta.setVariable( parameter.getVariable(), parameter.getValue() );
        }
      }
    }
    transMeta.activateParameters();
  }

  private void configureDataFlowOptions( BeamJobConfig config, DataflowPipelineOptions options, VariableSpace space ) throws IOException {

    List<String> files;
    if (StringUtils.isNotEmpty(config.getFatJar())) {
      files = new ArrayList<>(  );
      files.add(config.getFatJar());
    } else {
      files = BeamConst.findLibraryFilesToStage( null, space.environmentSubstitute( config.getPluginsToStage() ), true, true );
      files.removeIf( s-> s.contains( "commons-logging" ) || s.contains( "log4j" ) );
    }

    options.setFilesToStage( files );
    options.setProject( space.environmentSubstitute( config.getGcpProjectId() ) );
    options.setAppName( space.environmentSubstitute( config.getGcpAppName() ) );
    options.setStagingLocation( space.environmentSubstitute( config.getGcpStagingLocation() ) );
    if ( StringUtils.isNotEmpty( config.getGcpInitialNumberOfWorkers() ) ) {
      int numWorkers = Const.toInt( space.environmentSubstitute( config.getGcpInitialNumberOfWorkers() ), -1 );
      if ( numWorkers >= 0 ) {
        options.setNumWorkers( numWorkers );
      }
    }
    if ( StringUtils.isNotEmpty( config.getGcpMaximumNumberOfWokers() ) ) {
      int numWorkers = Const.toInt( space.environmentSubstitute( config.getGcpMaximumNumberOfWokers() ), -1 );
      if ( numWorkers >= 0 ) {
        options.setMaxNumWorkers( numWorkers );
      }
    }
    if ( StringUtils.isNotEmpty( config.getGcpWorkerMachineType() ) ) {
      String machineType = space.environmentSubstitute( config.getGcpWorkerMachineType() );
      options.setWorkerMachineType( machineType );
    }
    if ( StringUtils.isNotEmpty( config.getGcpWorkerDiskType() ) ) {
      String diskType = space.environmentSubstitute( config.getGcpWorkerDiskType() );
      options.setWorkerDiskType( diskType );
    }
    if ( StringUtils.isNotEmpty( config.getGcpDiskSizeGb() ) ) {
      int diskSize = Const.toInt( space.environmentSubstitute( config.getGcpDiskSizeGb() ), -1 );
      if ( diskSize >= 0 ) {
        options.setDiskSizeGb( diskSize );
      }
    }
    if ( StringUtils.isNotEmpty( config.getGcpZone() ) ) {
      String zone = space.environmentSubstitute( config.getGcpZone() );
      options.setZone( zone );
    }
    if ( StringUtils.isNotEmpty( config.getGcpRegion() ) ) {
      String region = space.environmentSubstitute( config.getGcpRegion() );
      options.setRegion( region );
    }
    if ( StringUtils.isNotEmpty( config.getGcpAutoScalingAlgorithm() ) ) {
      String algorithmCode = space.environmentSubstitute( config.getGcpAutoScalingAlgorithm() );
      try {

        DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType algorithm = DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.valueOf( algorithmCode );
        options.setAutoscalingAlgorithm( algorithm );
      } catch ( Exception e ) {
        logChannel.logError( "Unknown autoscaling algorithm for GCP Dataflow: " + algorithmCode, e );
      }
    }
    options.setStreaming( config.isGcpStreaming() );

  }

  private void configureSparkOptions( BeamJobConfig config, SparkPipelineOptions options, VariableSpace space, String transformationName ) throws IOException {

    // options.setFilesToStage( BeamConst.findLibraryFilesToStage( null, config.getPluginsToStage(), true, true ) );

    if ( StringUtils.isNotEmpty( config.getSparkMaster() ) ) {
      options.setSparkMaster( space.environmentSubstitute( config.getSparkMaster() ) );
    }
    if ( StringUtils.isNotEmpty( config.getSparkBatchIntervalMillis() ) ) {
      long interval = Const.toLong( space.environmentSubstitute( config.getSparkBatchIntervalMillis() ), -1L );
      if ( interval >= 0 ) {
        options.setBatchIntervalMillis( interval );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkCheckpointDir() ) ) {
      options.setCheckpointDir( space.environmentSubstitute( config.getSparkCheckpointDir() ) );
    }
    if ( StringUtils.isNotEmpty( config.getSparkCheckpointDurationMillis() ) ) {
      long duration = Const.toLong( space.environmentSubstitute( config.getSparkCheckpointDurationMillis() ), -1L );
      if ( duration >= 0 ) {
        options.setCheckpointDurationMillis( duration );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkMaxRecordsPerBatch() ) ) {
      long records = Const.toLong( space.environmentSubstitute( config.getSparkMaxRecordsPerBatch() ), -1L );
      if ( records >= 0 ) {
        options.setMaxRecordsPerBatch( records );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkMinReadTimeMillis() ) ) {
      long readTime = Const.toLong( space.environmentSubstitute( config.getSparkMinReadTimeMillis() ), -1L );
      if ( readTime >= 0 ) {
        options.setMinReadTimeMillis( readTime );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkReadTimePercentage() ) ) {
      double percentage = Const.toDouble( space.environmentSubstitute( config.getSparkReadTimePercentage() ), -1.0 );
      if ( percentage >= 0 ) {
        options.setReadTimePercentage( percentage / 100 );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkBundleSize() ) ) {
      long bundleSize = Const.toLong( space.environmentSubstitute( config.getSparkBundleSize() ), -1L );
      if ( bundleSize >= 0 ) {
        options.setBundleSize( bundleSize );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkStorageLevel() ) ) {
      options.setStorageLevel( space.environmentSubstitute( config.getSparkStorageLevel() ) );
    }
    String appName = transformationName.replace( " ", "_" );
    options.setAppName( appName );
  }

  private void configureFlinkOptions( BeamJobConfig config, FlinkPipelineOptions options, VariableSpace space ) throws IOException {

    options.setFilesToStage( BeamConst.findLibraryFilesToStage( null, config.getPluginsToStage(), true, true ) );

    // Address of the Flink Master where the Pipeline should be executed. Can either be of the form \"host:port\" or one of the special values [local], [collection] or [auto].")
    if (StringUtils.isNotEmpty( config.getFlinkMaster() )) {
      options.setFlinkMaster(space.environmentSubstitute( options.getFlinkMaster() ) );
    }

    // The degree of parallelism to be used when distributing operations onto workers. If the parallelism is not set, the configured Flink default is used, or 1 if none can be found.")
    if (StringUtils.isNotEmpty( config.getFlinkParallelism() )) {
      int value = Const.toInt( space.environmentSubstitute(config.getFlinkParallelism()), -1 );
      if (value>0) {
        options.setParallelism(value);
      }
    }

    // The interval in milliseconds at which to trigger checkpoints of the running pipeline. Default: No checkpointing.")
    if (StringUtils.isNotEmpty( config.getFlinkCheckpointingInterval() )) {
      long value = Const.toLong( space.environmentSubstitute( config.getFlinkCheckpointingInterval() ), -1L );
      if (value>0) {
        options.setCheckpointingInterval( value );
      }
    }

    // The checkpointing mode that defines consistency guarantee.")
    if (StringUtils.isNotEmpty( config.getFlinkCheckpointingMode() )) {
      String modeString = space.environmentSubstitute( config.getFlinkCheckpointingMode() );
      try {
        CheckpointingMode mode = CheckpointingMode.valueOf( modeString);
        if ( mode != null ) {
          options.setCheckpointingMode( modeString );
        }
      } catch(Exception e) {
        throw new IOException( "Unable to parse flink check pointing mode '"+modeString+"'", e );
      }
    }

    // The maximum time in milliseconds that a checkpoint may take before being discarded.")
    if (StringUtils.isNotEmpty( config.getFlinkCheckpointTimeoutMillis() )) {
      long value = Const.toLong( space.environmentSubstitute( config.getFlinkCheckpointTimeoutMillis() ), -1L );
      if (value>0) {
        options.setCheckpointTimeoutMillis( value );
      }
    }

    // The minimal pause in milliseconds before the next checkpoint is triggered.")
    if (StringUtils.isNotEmpty( config.getFlinkMinPauseBetweenCheckpoints() )) {
      long value = Const.toLong( space.environmentSubstitute( config.getFlinkMinPauseBetweenCheckpoints() ), -1L );
      if (value>0) {
        options.setMinPauseBetweenCheckpoints( value );
      }
    }

    // Sets the number of times that failed tasks are re-executed. A value of zero effectively disables fault tolerance. A value of -1 indicates that the system default value (as defined in the configuration) should be used.")
    if (StringUtils.isNotEmpty( config.getFlinkNumberOfExecutionRetries() )) {
      int value = Const.toInt( space.environmentSubstitute(config.getFlinkNumberOfExecutionRetries()), -1 );
      if (value>=0) {
        options.setNumberOfExecutionRetries( value );
      }
    }

    // Sets the delay in milliseconds between executions. A value of {@code -1} indicates that the default value should be used.")
    if (StringUtils.isNotEmpty( config.getFlinkExecutionRetryDelay() )) {
      long value = Const.toLong( space.environmentSubstitute( config.getFlinkExecutionRetryDelay() ), -1L );
      if (value>0) {
        options.setExecutionRetryDelay( value );
      }
    }

    // Sets the behavior of reusing objects.")
    if (StringUtils.isNotEmpty( config.getFlinkObjectReuse() )) {
      String str = space.environmentSubstitute( config.getFlinkObjectReuse() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setObjectReuse( value );
    }

    // Sets the state backend to use in streaming mode. Otherwise the default is read from the Flink config.")
    /** TODO
    if (StringUtils.isNotEmpty( config.getFlinkStateBackend() )) {
      String str = space.environmentSubstitute( config.getFlinkStateBackend() );
      try {

        options.setStateBackend(StateBackEnd);
      } catch(Exception e) {
        throw new IOException( "Unable to parse flink state back-end '"+modeString+"'", e );
      }
    }
    */

    // Enable/disable Beam metrics in Flink Runner")
    if (StringUtils.isNotEmpty( config.getFlinkEnableMetrics() )) {
      String str = space.environmentSubstitute( config.getFlinkEnableMetrics() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setEnableMetrics( value );
    }

    // Enables or disables externalized checkpoints. Works in conjunction with CheckpointingInterval")
    if (StringUtils.isNotEmpty( config.getFlinkExternalizedCheckpointsEnabled() )) {
      String str = space.environmentSubstitute( config.getFlinkExternalizedCheckpointsEnabled() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setExternalizedCheckpointsEnabled( value );
    }

    // Sets the behavior of externalized checkpoints on cancellation.")
    if (StringUtils.isNotEmpty( config.getFlinkRetainExternalizedCheckpointsOnCancellation() )) {
      String str = space.environmentSubstitute( config.getFlinkRetainExternalizedCheckpointsOnCancellation() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setRetainExternalizedCheckpointsOnCancellation( value );
    }

    // The maximum number of elements in a bundle.")
    if (StringUtils.isNotEmpty( config.getFlinkMaxBundleSize() )) {
      long value = Const.toLong( space.environmentSubstitute( config.getFlinkMaxBundleSize() ), -1L );
      if (value>0) {
        options.setMaxBundleSize( value );
      }
    }

    // The maximum time to wait before finalising a bundle (in milliseconds).")
    if (StringUtils.isNotEmpty( config.getFlinkMaxBundleTimeMills() )) {
      long value = Const.toLong( space.environmentSubstitute( config.getFlinkMaxBundleTimeMills() ), -1L );
      if ( value > 0 ) {
        options.setMaxBundleSize( value );
      }
    }

    // If set, shutdown sources when their watermark reaches +Inf.")
    if (StringUtils.isNotEmpty( config.getFlinkShutdownSourcesOnFinalWatermark() )) {
      String str = space.environmentSubstitute( config.getFlinkShutdownSourcesOnFinalWatermark() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setShutdownSourcesOnFinalWatermark( value );
    }

    // Interval in milliseconds for sending latency tracking marks from the sources to the sinks. Interval value <= 0 disables the feature.")
    if (StringUtils.isNotEmpty( config.getFlinkLatencyTrackingInterval() )) {
      long value = Const.toLong( space.environmentSubstitute( config.getFlinkLatencyTrackingInterval() ), -1L );
      if (value>0) {
        options.setLatencyTrackingInterval( value );
      }
    }

    // The interval in milliseconds for automatic watermark emission.")
    if (StringUtils.isNotEmpty( config.getFlinkAutoWatermarkInterval() )) {
      long value = Const.toLong( space.environmentSubstitute( config.getFlinkAutoWatermarkInterval() ), -1L );
      if (value>0) {
        options.setAutoWatermarkInterval( value );
      }
    }

    // Flink mode for data exchange of batch pipelines. Reference {@link org.apache.flink.api.common.ExecutionMode}.
    // Set this to BATCH_FORCED if pipelines get blocked, see https://issues.apache.org/jira/browse/FLINK-10672")
    if (StringUtils.isNotEmpty( config.getFlinkExecutionModeForBatch() )) {
      String modeString = space.environmentSubstitute( config.getFlinkExecutionModeForBatch() );
      ExecutionMode mode = ExecutionMode.valueOf( modeString );
      try {
        options.setExecutionModeForBatch( modeString );
      } catch(Exception e) {
        throw new IOException( "Unable to parse flink execution mode for batch '"+modeString+"'", e );
      }
    }
  }


  /**
   * Gets updatedListeners
   *
   * @return value of updatedListeners
   */
  public List<BeamMetricsUpdatedListener> getUpdatedListeners() {
    return updatedListeners;
  }

  /**
   * @param updatedListeners The updatedListeners to set
   */
  public void setUpdatedListeners( List<BeamMetricsUpdatedListener> updatedListeners ) {
    this.updatedListeners = updatedListeners;
  }

  /**
   * Gets loggingMetrics
   *
   * @return value of loggingMetrics
   */
  public boolean isLoggingMetrics() {
    return loggingMetrics;
  }

  /**
   * @param loggingMetrics The loggingMetrics to set
   */
  public void setLoggingMetrics( boolean loggingMetrics ) {
    this.loggingMetrics = loggingMetrics;
  }

  /**
   * Gets logChannel
   *
   * @return value of logChannel
   */
  public LogChannelInterface getLogChannel() {
    return logChannel;
  }

  /**
   * @param logChannel The logChannel to set
   */
  public void setLogChannel( LogChannelInterface logChannel ) {
    this.logChannel = logChannel;
  }

  /**
   * Gets sparkContext
   *
   * @return value of sparkContext
   */
  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }

  /**
   * @param sparkContext The sparkContext to set
   */
  public void setSparkContext( JavaSparkContext sparkContext ) {
    this.sparkContext = sparkContext;
  }
}
