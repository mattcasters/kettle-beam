package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.shared.VariableValue;
import org.kettle.beam.core.transform.GroupByTransform;
import org.kettle.beam.core.transform.StepTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.core.util.KettleBeamUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.di.trans.steps.memgroupby.MemoryGroupByMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BeamGenericStepHandler implements BeamStepHandler {

  private IMetaStore metaStore;
  private String metaStoreJson;
  private TransMeta transMeta;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  public BeamGenericStepHandler( IMetaStore metaStore, String metaStoreJson, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.metaStore = metaStore;
    this.metaStoreJson = metaStoreJson;
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
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input  ) throws KettleException {

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

    // Send all the information on their way to the right nodes
    //
    StepTransform stepTransform = new StepTransform( variableValues, metaStoreJson, stepPluginClasses, xpPluginClasses,
      stepMeta.getName(), stepMeta.getStepID(), stepMetaInterfaceXml, JsonRowMeta.toJson( rowMeta ), targetSteps, infoSteps, infoRowMetaJsons, infoCollectionViews );


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


  private List<VariableValue> getVariableValues( VariableSpace space ) {

    List<VariableValue> variableValues = new ArrayList<>();
    for ( String variable : space.listVariables() ) {
      String value = space.getVariable( variable );
      variableValues.add( new VariableValue( variable, value ) );
    }
    return variableValues;
  }
}
