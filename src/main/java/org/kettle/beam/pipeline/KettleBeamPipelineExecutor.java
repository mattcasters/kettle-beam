package org.kettle.beam.pipeline;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.metastore.JobParameter;
import org.kettle.beam.metastore.RunnerType;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class KettleBeamPipelineExecutor {

  private LogChannelInterface log;
  private TransMeta transMeta;
  private BeamJobConfig jobConfig;
  private IMetaStore metaStore;
  private ClassLoader classLoader;
  private List<BeamMetricsUpdatedListener> updatedListeners;
  private boolean loggingMetrics;

  private KettleBeamPipelineExecutor() {
    this.updatedListeners = new ArrayList<>(  );
  }

  public KettleBeamPipelineExecutor( LogChannelInterface log, TransMeta transMeta, BeamJobConfig jobConfig, IMetaStore metaStore, ClassLoader classLoader ) {
    this();
    this.log = log;
    this.transMeta = transMeta;
    this.jobConfig = jobConfig;
    this.metaStore = metaStore;
    this.classLoader = classLoader;
    this.loggingMetrics = true;
  }

  public PipelineResult execute() throws KettleException {
    ClassLoader oldContextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Explain to various classes in the Beam API (@see org.apache.beam.sdk.io.FileSystems)
      // what the context classloader is.
      // Set it back when we're done here.
      //
      Thread.currentThread().setContextClassLoader( classLoader );

      final Pipeline pipeline = getPipeline( transMeta, jobConfig );

      PipelineResult pipelineResult = pipeline.run();

      Timer timer = new Timer();
      TimerTask timerTask = new TimerTask() {
        @Override public void run() {

          // Log the metrics...
          //
          if (isLoggingMetrics()) {
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

      log.logBasic( "  ----------------- End of Beam job " + pipeline.getOptions().getJobName() + " -----------------------" );

      return pipelineResult;
    } finally {
      Thread.currentThread().setContextClassLoader( oldContextClassLoader );
    }

  }

  private void logMetrics( PipelineResult pipelineResult ) {
    MetricResults metricResults = pipelineResult.metrics();

    log.logBasic( "  ----------------- Metrics refresh @ " + new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss" ).format( new Date() ) + " -----------------------" );

    MetricQueryResults allResults = metricResults.queryMetrics( MetricsFilter.builder().build() );
    for ( MetricResult<Long> result : allResults.getCounters() ) {
      log.logBasic( "Name: " + result.getName() + " Attempted: " + result.getAttempted() + " Committed: " + result.getCommitted() );
    }
  }

  public void updateListeners(PipelineResult pipelineResult) {
    for (BeamMetricsUpdatedListener listener : updatedListeners) {
      listener.beamMetricsUpdated(pipelineResult);
    }
  }

  public Pipeline getPipeline( TransMeta transMeta, BeamJobConfig config ) throws KettleException {

    try {

      if ( StringUtils.isEmpty( config.getRunnerTypeName() ) ) {
        throw new KettleException( "You need to specify a runner type, one of : " + RunnerType.values().toString() );
      }
      PipelineOptions pipelineOptions = null;
      Class<? extends PipelineRunner<?>> pipelineRunnerClass = null;
      VariableSpace space = transMeta;

      RunnerType runnerType = RunnerType.getRunnerTypeByName( transMeta.environmentSubstitute( config.getRunnerTypeName() ) );
      switch ( runnerType ) {
        case Direct:
          pipelineOptions = PipelineOptionsFactory.create();
          pipelineRunnerClass = DirectRunner.class;
          break;
        case DataFlow:
          DataflowPipelineOptions dfOptions = PipelineOptionsFactory.as( DataflowPipelineOptions.class );
          configureDataFlowOptions( config, dfOptions, space );
          pipelineOptions = dfOptions;
          pipelineRunnerClass = DataflowRunner.class;
          break;
        case Spark:
          SparkPipelineOptions sparkOptions = PipelineOptionsFactory.as( SparkPipelineOptions.class );
          configureSparkOptions( config, sparkOptions, space );
          pipelineOptions = sparkOptions;
          pipelineRunnerClass = SparkRunner.class;
          break;
        case Flink:
          FlinkPipelineOptions flinkOptions = PipelineOptionsFactory.as( FlinkPipelineOptions.class );
          configureFlinkOptions( config, flinkOptions, space );
          pipelineOptions = flinkOptions;
          pipelineRunnerClass = FlinkRunner.class;
          log.logError( "Still missing a lot of options for Spark, this will probably fail" );
          break;
        default:
          throw new KettleException( "Sorry, this isn't implemented yet" );
      }

      configureStandardOptions( config, transMeta.getName(), pipelineOptions, space );


      setVariablesInTransformation( config, transMeta );

      TransMetaPipelineConverter converter = new TransMetaPipelineConverter( transMeta, metaStore, config.getPluginsToStage() );
      Pipeline pipeline = converter.createPipeline( pipelineRunnerClass, pipelineOptions );

      return pipeline;
    } catch ( Exception e ) {
      throw new KettleException( "Error configuring local Beam Engine", e );
    }

  }

  private void configureStandardOptions( BeamJobConfig config, String transformationName, PipelineOptions pipelineOptions, VariableSpace space ) {
    if ( StringUtils.isNotEmpty( transformationName ) ) {
      String sanitizedName = transformationName.replace( " ", "_" );
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

    options.setFilesToStage( BeamConst.findLibraryFilesToStage( null, space.environmentSubstitute( config.getPluginsToStage() ), true, true ) );
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
        log.logError( "Unknown autoscaling algorithm for GCP Dataflow: " + algorithmCode, e );
      }
    }
    options.setStreaming( config.isGcpStreaming() );

  }

  private void configureSparkOptions( BeamJobConfig config, SparkPipelineOptions options, VariableSpace space ) throws IOException {

    options.setFilesToStage( BeamConst.findLibraryFilesToStage( null, config.getPluginsToStage(), true, true ) );

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
  }

  private void configureFlinkOptions( BeamJobConfig config, FlinkPipelineOptions options, VariableSpace space ) throws IOException {

    // TODO: Do the other things as well
    options.setFilesToStage( BeamConst.findLibraryFilesToStage( null, config.getPluginsToStage(), true, true ) );

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
}
