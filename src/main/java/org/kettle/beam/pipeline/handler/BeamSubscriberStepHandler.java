package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamSubscribeTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.steps.pubsub.BeamSubscribeMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamSubscriberStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamSubscriberStepHandler( BeamJobConfig beamJobConfig, IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( beamJobConfig, true, false, metaStore, transMeta, stepPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input ) throws KettleException {

    // A Beam subscriber step
    //
    BeamSubscribeMeta inputMeta = (BeamSubscribeMeta) stepMeta.getStepMetaInterface();

    RowMetaInterface outputRowMeta = transMeta.getStepFields( stepMeta );
    String rowMetaJson = JsonRowMeta.toJson( outputRowMeta );

    // Verify some things:
    //
    if ( StringUtils.isEmpty( inputMeta.getTopic() ) ) {
      throw new KettleException( "Please specify a topic to read from in Beam Pub/Sub Subscribe step '" + stepMeta.getName() + "'" );
    }

    BeamSubscribeTransform subscribeTransform = new BeamSubscribeTransform(
      stepMeta.getName(),
      stepMeta.getName(),
      transMeta.environmentSubstitute( inputMeta.getSubscription() ),
      transMeta.environmentSubstitute( inputMeta.getTopic() ),
      inputMeta.getMessageType(),
      rowMetaJson,
      stepPluginClasses,
      xpPluginClasses
    );

    PCollection<KettleRow> afterInput = pipeline.apply( subscribeTransform );
    stepCollectionMap.put( stepMeta.getName(), afterInput );

    log.logBasic( "Handled step (SUBSCRIBE) : " + stepMeta.getName() );
  }
}
