package org.kettle.beam.xp;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.pipeline.KettleBeamPipelineExecutor;
import org.pentaho.di.ExecutionConfiguration;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransListener;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

@ExtensionPoint( id = "RunBeamTransExecutionPoint", extensionPointId = "BeamRunConfigurationExecution",
  description = "Runs the transformation using the beam run config in Spoon if necessary" )
public class RunBeamTransExecutionPoint implements ExtensionPointInterface {

  public static final String KETTLE_BEAM_PIPELINE_EXECUTOR = "KETTLE_BEAM_PIPELINE_EXECUTOR";

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) {

    Object[] arguments = (Object[]) object;

    VariableSpace space;
    TransMeta transMeta;

    // See if we're using the Trans job entry
    // Who came up with this shit?
    //
    if ( arguments[ 2 ] instanceof JobMeta ) {
      space = (VariableSpace) arguments[ 2 ];
      if ( arguments[ 3 ] instanceof JobMeta ) {
        // Doing something stupid in the GUI
        // Why????
        //
        return;
      }
      transMeta = (TransMeta) arguments[ 3 ];
    } else {
      space = (VariableSpace) arguments[ 3 ];
      transMeta = (TransMeta) arguments[ 2 ];
    }
    String beamJobConfigName = (String) arguments[ 0 ];
    // TODO: do something with the exec config
    //
    ExecutionConfiguration executionConfiguration = (ExecutionConfiguration) arguments[ 1 ];
    Repository repository = (Repository) arguments[ 4 ];

    log.logBasic( "Executing transformation " + transMeta.getName() + " on Beam Job Config " + beamJobConfigName );

    // Disable any unit test (data sets) logic
    // TODO: find a generic way of doing this. Can the "data sets" plugin detect the Run Config?
    //
    transMeta.setVariable( "__UnitTest_Run__", "N" );

    try {
      // Load the job config.
      //
      IMetaStore metaStore = transMeta.getMetaStore();
      MetaStoreFactory<BeamJobConfig> configFactory = new MetaStoreFactory<>( BeamJobConfig.class, metaStore, PentahoDefaults.NAMESPACE );
      BeamJobConfig beamJobConfig = configFactory.loadElement( beamJobConfigName );

      // Just try to run it...
      //
      KettleBeamPipelineExecutor executor = new KettleBeamPipelineExecutor( log, transMeta, beamJobConfig, metaStore, RunBeamTransExecutionPoint.this.getClass().getClassLoader() );

      // Make sure we're not running in spoon and not in a spoon job entry
      //
      Spoon spoon = Spoon.getInstance();
      if ( spoon != null && spoon.getActiveTransGraph() != null ) {

        executeInSpoon( executor, transMeta, repository, space );

      } else {
        // Simply execute it
        //
        executor.execute();
      }


    } catch ( Exception e ) {
      log.logError( "Error executing transformation on Beam job configuration '" + beamJobConfigName + "'", e );
    }
  }

  private void executeInSpoon( final KettleBeamPipelineExecutor executor, TransMeta transMeta, Repository repository, VariableSpace space ) throws KettleException {
    Spoon spoon = Spoon.getInstance();

    // Open the results pane and the logging...
    // This would also happen with a regular transformation...
    //
    TransGraph transGraph = spoon.getActiveTransGraph();

    if ( !transGraph.isExecutionResultsPaneVisible() ) {
      transGraph.showExecutionResults();
    } else {
      transGraph.showExecutionResults();
      transGraph.showExecutionResults();
    }

    // Create a new mock Trans object
    //
    TransMeta copyTransMeta = copyCleanTransMeta( transMeta, repository, spoon.getMetaStore(), space );
    BeamTrans trans = new BeamTrans( copyTransMeta );
    trans.prepareExecution( null );
    trans.setRunning( false );
    trans.setPreparing( false );
    trans.setInitializing( true );


    // Correct the logging channel in the executor...
    //
    executor.setLogChannel( trans.getLogChannel() );
    executor.setLoggingMetrics( true );

    // Set all the steps running
    //
    for ( StepMetaDataCombi combi : trans.getSteps() ) {
      BeamDummyTrans beamStep = new BeamDummyTrans( combi.stepMeta, combi.data, combi.copy, copyTransMeta, trans );
      combi.step = beamStep;
      beamStep.setRunning( true );
      beamStep.setStopped( false );
      beamStep.setInit( true );
    }

    // Pass this one...
    transGraph.setTrans( trans );

    // Store a link to the pipeline in Trans for 3rd party plugins or other XP
    //
    trans.getExtensionDataMap().put( KETTLE_BEAM_PIPELINE_EXECUTOR, executor );

    // Before we start, attach a metrics listener so we can update the GUI
    //
    executor.getUpdatedListeners().add( pipelineResult -> metricsUpdated( pipelineResult, trans ) );

    // Start the execution in a different thread to not block the UI...
    //
    Runnable runnable = () -> {
      try {
        PipelineResult pipelineResult = executor.execute();
        if (pipelineResult!=null) {
          trans.setRunning( false );
          for ( StepMetaDataCombi combi : trans.getSteps() ) {
            combi.step.setRunning( false );
            combi.step.markStop();
          }
          metricsUpdated( pipelineResult, trans );
          for ( TransListener listener : trans.getTransListeners() ) {
            listener.transFinished( trans );
          }
        }
      } catch ( Exception e ) {
        spoon.getDisplay().asyncExec( () -> new ErrorDialog( spoon.getShell(), "Error", "There was an error building or executing the pipeline", e ) );
      }
    };

    // Create a new thread on the class loader
    //
    Thread thread = new Thread( runnable );
    thread.start();

  }


  // This gets called every 10 seconds or so
  //
  private void metricsUpdated( PipelineResult pipelineResult, BeamTrans trans ) {
    LogChannelInterface log = trans.getLogChannel();
    log.logBasic( "PIPELINE STATE: " + pipelineResult.getState().name() );
    boolean cancelPipeline = false;
    switch ( pipelineResult.getState() ) {
      case DONE:
        trans.setRunning( false );
        trans.setFinished( true );
        trans.setInitializing( false );
        log.logBasic( "Transformation finished.");
        break;
      case STOPPED:
      case CANCELLED:
        trans.setStopped( true );
        cancelPipeline = true;
        break;
      case FAILED:
        trans.setStopped( true );
        trans.setFinished( true );
        trans.setInitializing( false );
        log.logBasic( "Transformation failed.");
        cancelPipeline = true;
        break;
      case UNKNOWN:
        break;
      case UPDATED:
      case RUNNING:
        trans.setRunning( true );
        trans.setInitializing( false );
        trans.setStopped( false );
        break;
      default:
        break;
    }

    if (cancelPipeline) {
      try {
        PipelineResult.State cancel = pipelineResult.cancel();


        log.logBasic( "Pipeline cancelled" );
      } catch(Exception e) {
        log.logError( "Cancellation of pipeline failed", e );
      }
    }

    MetricResults metricResults = pipelineResult.metrics();

    MetricQueryResults allResults = metricResults.queryMetrics( MetricsFilter.builder().build() );

    boolean first = true;

    for ( MetricResult<Long> result : allResults.getCounters() ) {

      // Once we see counter values, init is done.
      //
      if ( first && trans.isInitializing() ) {
        first = false;
        trans.setInitializing( false );
        log.logBasic( "Initialisation is done!" );
        // Mark start on all steps...
        //
        for ( StepMetaDataCombi combi : trans.getSteps() ) {
          ( (BeamDummyTrans) combi.step ).setInit( false );
          combi.step.markStart();
        }
      }

      String metricsType = result.getName().getNamespace();
      String metricsName = result.getName().getName();
      long processed = result.getAttempted();

      StepMetaDataCombi combi = findCombi( trans, metricsName );
      if ( combi != null ) {
        BaseStep bs = (BaseStep) combi.step;

        if ( "read".equalsIgnoreCase( metricsType ) ) {
          bs.setLinesRead( processed );
        } else if ( "written".equalsIgnoreCase( metricsType ) ) {
          bs.setLinesWritten( processed );
        } else if ( "input".equalsIgnoreCase( metricsType ) ) {
          bs.setLinesInput( processed );
        } else if ( "output".equalsIgnoreCase( metricsType ) ) {
          bs.setLinesOutput( processed );
        } else if ( "init".equalsIgnoreCase( metricsType ) ) {
          bs.setCopy( (int) processed );
        }

        // Set the step status to reflect the pipeline status.
        //
        switch ( pipelineResult.getState() ) {
          case DONE:
            bs.setRunning( false );
            combi.data.setStatus( BaseStepData.StepExecutionStatus.STATUS_DISPOSED );
            break;
          case CANCELLED:
          case FAILED:
          case STOPPED:
            bs.setStopped( true );
            bs.setRunning( false );
            break;
          case RUNNING:
            bs.setRunning( true );
            bs.setStopped( false );
            break;
          case UNKNOWN:
            break;
          case UPDATED:
            break;
          default:
            break;
        }
      }
    }
  }

  private StepMetaDataCombi findCombi( Trans trans, String metricsName ) {
    for ( StepMetaDataCombi combi : trans.getSteps() ) {
      if ( combi.stepname.equals( metricsName ) ) {
        return combi;
      }
    }
    return null;
  }


  private TransMeta copyCleanTransMeta( TransMeta transMeta, Repository repository, IMetaStore metaStore, VariableSpace space ) throws KettleException {

    try {
      String transMetaXml = transMeta.getXML();
      TransMeta copy = new TransMeta();
      copy.setMetaStore( metaStore );
      copy.loadXML( XMLHandler.loadXMLString( transMetaXml, TransMeta.XML_TAG ), null, metaStore, repository, true, space, null );

      for ( StepMeta stepMeta : copy.getSteps() ) {
        stepMeta.setCopies( 1 );

        DummyTransMeta dummyTransMeta = new DummyTransMeta();

        // Replace all stepMeta with a Dummy with the same name
        //
        stepMeta.setStepID( "Dummy" );
        stepMeta.setStepMetaInterface( dummyTransMeta );
      }

      return copy;

    } catch ( Exception e ) {
      throw new KettleException( "Error copying/cleaning transformation metadata", e );
    }
  }

}
