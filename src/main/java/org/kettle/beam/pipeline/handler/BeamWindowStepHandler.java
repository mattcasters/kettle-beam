package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Duration;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.WindowInfoFn;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.steps.window.BeamWindowMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamWindowStepHandler implements BeamStepHandler {

  private IMetaStore metaStore;
  private TransMeta transMeta;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  public BeamWindowStepHandler( IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.metaStore = metaStore;
    this.transMeta = transMeta;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  public boolean isInput() {
    return false;
  }

  public boolean isOutput() {
    return false;
  }

  @Override public void handleStep( LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface inputRowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input ) throws KettleException {

    BeamWindowMeta beamWindowMeta = (BeamWindowMeta) stepMeta.getStepMetaInterface();

    if ( StringUtils.isEmpty( beamWindowMeta.getWindowType() ) ) {
      throw new KettleException( "Please specify a window type in Beam Window step '" + stepMeta.getName() + "'" );
    }

    String duration = transMeta.environmentSubstitute( beamWindowMeta.getDuration() );
    long durationSeconds = Const.toLong( duration, -1L );

    PCollection<KettleRow> stepPCollection;

    if ( BeamDefaults.WINDOW_TYPE_FIXED.equals( beamWindowMeta.getWindowType() ) ) {

      if ( durationSeconds <= 0 ) {
        throw new KettleException( "Please specify a valid positive window size (duration) for Beam window step '" + stepMeta.getName() + "'" );
      }

      FixedWindows fixedWindows = FixedWindows
        .of( Duration.standardSeconds( durationSeconds ) );
      stepPCollection = input.apply( Window.into( fixedWindows ) );

    } else if ( BeamDefaults.WINDOW_TYPE_SLIDING.equals( beamWindowMeta.getWindowType() ) ) {

      if ( durationSeconds <= 0 ) {
        throw new KettleException( "Please specify a valid positive window size (duration) for Beam window step '" + stepMeta.getName() + "'" );
      }

      String every = transMeta.environmentSubstitute( beamWindowMeta.getEvery() );
      long everySeconds = Const.toLong( every, -1L );

      SlidingWindows slidingWindows = SlidingWindows
        .of( Duration.standardSeconds( durationSeconds ) )
        .every( Duration.standardSeconds( everySeconds ) );
      stepPCollection = input.apply( Window.into( slidingWindows ) );

    } else if ( BeamDefaults.WINDOW_TYPE_SESSION.equals( beamWindowMeta.getWindowType() ) ) {

      if ( durationSeconds < 600 ) {
        throw new KettleException(
          "Please specify a window size (duration) of at least 600 (10 minutes) for Beam window step '" + stepMeta.getName() + "'.  This is the minimum gap between session windows." );
      }

      Sessions sessionWindows = Sessions
        .withGapDuration( Duration.standardSeconds( durationSeconds ) );
      stepPCollection = input.apply( Window.into( sessionWindows ) );

    } else if ( BeamDefaults.WINDOW_TYPE_GLOBAL.equals( beamWindowMeta.getWindowType() ) ) {

      stepPCollection = input.apply( Window.into( new GlobalWindows() ) );

    } else {
      throw new KettleException( "Beam Window type '" + beamWindowMeta.getWindowType() + " is not supported in step '" + stepMeta.getName() + "'" );
    }

    // Now get window information about the window if we asked about it...
    //
    if ( StringUtils.isNotEmpty( beamWindowMeta.getStartWindowField() ) ||
      StringUtils.isNotEmpty( beamWindowMeta.getEndWindowField() ) ||
      StringUtils.isNotEmpty( beamWindowMeta.getMaxWindowField() ) ) {

      WindowInfoFn windowInfoFn = new WindowInfoFn(
        stepMeta.getName(),
        transMeta.environmentSubstitute( beamWindowMeta.getMaxWindowField() ),
        transMeta.environmentSubstitute( beamWindowMeta.getStartWindowField() ),
        transMeta.environmentSubstitute( beamWindowMeta.getMaxWindowField() ),
        JsonRowMeta.toJson( inputRowMeta ),
        stepPluginClasses,
        xpPluginClasses
      );

      stepPCollection = stepPCollection.apply( ParDo.of(windowInfoFn) );
    }

    // Save this in the map
    //
    stepCollectionMap.put( stepMeta.getName(), stepPCollection );
    log.logBasic( "Handled step (WINDOW) : " + stepMeta.getName() + ", gets data from " + previousSteps.size() + " previous step(s)" );
  }
}
