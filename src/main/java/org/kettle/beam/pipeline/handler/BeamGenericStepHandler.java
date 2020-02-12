package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.StringToKettleRowFn;
import org.kettle.beam.core.partition.SinglePartitionFn;
import org.kettle.beam.core.shared.VariableValue;
import org.kettle.beam.core.transform.StepBatchTransform;
import org.kettle.beam.core.transform.StepTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.core.util.KettleBeamUtil;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.metastore.api.IMetaStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BeamGenericStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  private String metaStoreJson;

  public BeamGenericStepHandler( BeamJobConfig beamJobConfig, IMetaStore metaStore, String metaStoreJson, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( beamJobConfig, false, false, metaStore, transMeta, stepPluginClasses, xpPluginClasses );
    this.metaStoreJson = metaStoreJson;
  }

  @Override public void handleStep( LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input ) throws KettleException {

    // If we have no previous step, it's an input step.  We need to start from pipeline
    //
    boolean inputStep = input == null;
    boolean reduceParallelism = checkStepCopiesForReducedParallelism( stepMeta );
    reduceParallelism=reduceParallelism || needsSingleThreading( stepMeta );

    String stepMetaInterfaceXml = XMLHandler.openTag( StepMeta.XML_TAG ) + stepMeta.getStepMetaInterface().getXML() + XMLHandler.closeTag( StepMeta.XML_TAG );


    // See if the step has Info steps
    //
    List<StepMeta> infoStepMetas = transMeta.findPreviousSteps( stepMeta, true );
    List<String> infoSteps = new ArrayList<>();
    List<String> infoRowMetaJsons = new ArrayList<>();
    List<PCollectionView<List<KettleRow>>> infoCollectionViews = new ArrayList<>();
    for ( StepMeta infoStepMeta : infoStepMetas ) {
      if ( !previousSteps.contains( infoStepMeta ) ) {
        infoSteps.add( infoStepMeta.getName() );
        infoRowMetaJsons.add( JsonRowMeta.toJson( transMeta.getStepFields( infoStepMeta ) ) );
        PCollection<KettleRow> infoCollection = stepCollectionMap.get( infoStepMeta.getName() );
        if ( infoCollection == null ) {
          throw new KettleException( "Unable to find collection for step '" + infoStepMeta.getName() + " providing info for '" + stepMeta.getName() + "'" );
        }
        infoCollectionViews.add( infoCollection.apply( View.asList() ) );
      }
    }

    // Get the list of variables from the TransMeta variable space:
    //
    List<VariableValue> variableValues = getVariableValues( transMeta );

    // Find out all the target steps for this step...
    //
    StepIOMetaInterface ioMeta = stepMeta.getStepMetaInterface().getStepIOMeta();
    List<String> targetSteps = new ArrayList<String>();
    for ( StreamInterface targetStream : ioMeta.getTargetStreams() ) {
      if ( targetStream.getStepMeta() != null ) {
        targetSteps.add( targetStream.getStepMeta().getName() );
      }
    }

    // For streaming pipelines we need to flush the rows in the buffer of a generic step (Table Output, Neo4j Output, ...)
    // This is what the BeamJobConfig option "Streaming Kettle Steps Flush Interval" is for...
    // Without a valid value we default to -1 to disable flushing.
    //
    int flushIntervalMs = Const.toInt(beamJobConfig.getStreamingKettleStepsFlushInterval(), -1);

    // Send all the information on their way to the right nodes
    //
    PTransform<PCollection<KettleRow>, PCollectionTuple> stepTransform;
    if (needsBatching(stepMeta)) {
      stepTransform = new StepBatchTransform( variableValues, metaStoreJson, stepPluginClasses, xpPluginClasses, transMeta.getSizeRowset(), flushIntervalMs,
        stepMeta.getName(), stepMeta.getStepID(), stepMetaInterfaceXml, JsonRowMeta.toJson( rowMeta ), inputStep,
        targetSteps, infoSteps, infoRowMetaJsons, infoCollectionViews );
    } else {
      stepTransform = new StepTransform( variableValues, metaStoreJson, stepPluginClasses, xpPluginClasses, transMeta.getSizeRowset(), flushIntervalMs,
        stepMeta.getName(), stepMeta.getStepID(), stepMetaInterfaceXml, JsonRowMeta.toJson( rowMeta ), inputStep,
        targetSteps, infoSteps, infoRowMetaJsons, infoCollectionViews );
    }

    if ( input == null ) {
      // Start from a dummy row and group over it.
      // Trick Beam into only running a single thread of the step that comes next.
      //
      input = pipeline
        .apply( Create.of( Arrays.asList( "kettle-single-value" ) ) ).setCoder( StringUtf8Coder.of() )
        .apply( WithKeys.of( (Void) null ) )
        .apply( GroupByKey.create() )
        .apply( Values.create() )
        .apply( Flatten.iterables() )
        .apply( ParDo.of( new StringToKettleRowFn( stepMeta.getName(), JsonRowMeta.toJson( rowMeta ), stepPluginClasses, xpPluginClasses ) ) );

      // Store this new collection so we can hook up other steps...
      //
      String tupleId = KettleBeamUtil.createMainInputTupleId( stepMeta.getName() );
      stepCollectionMap.put( tupleId, input );
    } else if ( reduceParallelism ) {
      PCollection.IsBounded isBounded = input.isBounded();
      if (isBounded== PCollection.IsBounded.BOUNDED) {
        // group across all fields to get down to a single thread...
        //
        input = input.apply( WithKeys.of( (Void) null ) )
          .setCoder( KvCoder.of( VoidCoder.of(), input.getCoder() ) )
          .apply( GroupByKey.create() )
          .apply( Values.create() )
          .apply( Flatten.iterables() )
        ;
      } else {

        // Streaming: try partitioning over a single partition
        // NOTE: doesn't seem to work <sigh/>
        /*
        input = input
          .apply( Partition.of( 1, new SinglePartitionFn() ) )
          .apply( Flatten.pCollections() )
        ;
         */
        throw new KettleException( "Unable to reduce parallel in an unbounded (streaming) pipeline in step : "+stepMeta.getName() );
      }
    }

    // Apply the step transform to the previous io step PCollection(s)
    //
    PCollectionTuple tuple = input.apply( stepMeta.getName(), stepTransform );

    // The main collection
    //
    PCollection<KettleRow> mainPCollection = tuple.get( new TupleTag<KettleRow>( KettleBeamUtil.createMainOutputTupleId( stepMeta.getName() ) ) );

    // Save this in the map
    //
    stepCollectionMap.put( stepMeta.getName(), mainPCollection );

    // Were there any targeted steps in this step?
    //
    for ( String targetStep : targetSteps ) {
      String tupleId = KettleBeamUtil.createTargetTupleId( stepMeta.getName(), targetStep );
      PCollection<KettleRow> targetPCollection = tuple.get( new TupleTag<KettleRow>( tupleId ) );

      // Store this in the map as well
      //
      stepCollectionMap.put( tupleId, targetPCollection );
    }

    log.logBasic( "Handled step (STEP) : " + stepMeta.getName() + ", gets data from " + previousSteps.size() + " previous step(s), targets=" + targetSteps.size() + ", infos=" + infoSteps.size() );
  }

  public static boolean needsBatching( StepMeta stepMeta ) {
    String value = stepMeta.getAttribute( BeamConst.STRING_KETTLE_BEAM, BeamConst.STRING_STEP_FLAG_BATCH );
    return value!=null && "true".equalsIgnoreCase( value );
  }

  public static boolean needsSingleThreading( StepMeta stepMeta ) {
    String value = stepMeta.getAttribute( BeamConst.STRING_KETTLE_BEAM, BeamConst.STRING_STEP_FLAG_SINGLE_THREADED );
    return value!=null && "true".equalsIgnoreCase( value );
  }

  private boolean checkStepCopiesForReducedParallelism( StepMeta stepMeta ) {
    if ( stepMeta.getCopiesString() == null ) {
      return false;
    }
    String copiesString = stepMeta.getCopiesString();

    String[] keyWords = new String[] { "BEAM_SINGLE", "SINGLE_BEAM", "BEAM_OUTPUT", "OUTPUT" };

    for ( String keyWord : keyWords ) {
      if ( copiesString.equalsIgnoreCase( keyWord ) ) {
        return true;
      }
    }
    return false;
  }


  private List<VariableValue> getVariableValues( VariableSpace space ) {

    List<VariableValue> variableValues = new ArrayList<>();
    for ( String variable : space.listVariables() ) {
      String value = space.getVariable( variable );
      variableValues.add( new VariableValue( variable, value ) );
    }
    return variableValues;
  }
}
