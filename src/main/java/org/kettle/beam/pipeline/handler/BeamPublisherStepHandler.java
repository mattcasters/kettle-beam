package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamInputTransform;
import org.kettle.beam.core.transform.BeamOutputTransform;
import org.kettle.beam.core.transform.BeamPublishTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.io.BeamInputMeta;
import org.kettle.beam.steps.io.BeamOutputMeta;
import org.kettle.beam.steps.pubsub.BeamPublishMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamPublisherStepHandler implements BeamStepHandler {

  private IMetaStore metaStore;
  private TransMeta transMeta;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  public BeamPublisherStepHandler( IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.metaStore = metaStore;
    this.transMeta = transMeta;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  public boolean isInput() {
    return false;
  }

  public boolean isOutput() {
    return true;
  }

  @Override public void handleStep( LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input  ) throws KettleException {

    BeamPublishMeta publishMeta = (BeamPublishMeta) stepMeta.getStepMetaInterface();

    // some validation
    //
    if ( StringUtils.isEmpty(publishMeta.getTopic())) {
      throw new KettleException( "Please specify a topic to publish to in Beam Pub/Sub Publish step '"+stepMeta.getName()+"'" );
    }

    BeamPublishTransform beamOutputTransform = new BeamPublishTransform(
      stepMeta.getName(),
      transMeta.environmentSubstitute( publishMeta.getTopic() ),
      publishMeta.getMessageType(),
      publishMeta.getMessageField(),
      JsonRowMeta.toJson(rowMeta),
      stepPluginClasses,
      xpPluginClasses
    );

    // Which step do we apply this transform to?
    // Ignore info hops until we figure that out.
    //
    if ( previousSteps.size() > 1 ) {
      throw new KettleException( "Combining data from multiple steps is not supported yet!" );
    }
    StepMeta previousStep = previousSteps.get( 0 );

    // No need to store this, it's PDone.
    //
    input.apply( beamOutputTransform );
    log.logBasic( "Handled step (PUBLISH) : " + stepMeta.getName() + ", gets data from " + previousStep.getName() );
  }
}
