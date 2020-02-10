package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamKafkaInputTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.steps.kafka.BeamConsumeMeta;
import org.kettle.beam.steps.kafka.ConfigOption;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamKafkaInputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamKafkaInputStepHandler( BeamJobConfig beamJobConfig, IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( beamJobConfig, true, false, metaStore, transMeta, stepPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input ) throws KettleException {

    // Input handling
    //
    BeamConsumeMeta beamConsumeMeta = (BeamConsumeMeta) stepMeta.getStepMetaInterface();

    // Output rows (fields selection)
    //
    RowMetaInterface outputRowMeta = new RowMeta();
    beamConsumeMeta.getFields( outputRowMeta, stepMeta.getName(), null, null, transMeta, null, null );

    String[] parameters = new String[beamConsumeMeta.getConfigOptions().size()];
    String[] values = new String[beamConsumeMeta.getConfigOptions().size()];
    String[] types = new String[beamConsumeMeta.getConfigOptions().size()];
    for (int i=0;i<parameters.length;i++) {
      ConfigOption option = beamConsumeMeta.getConfigOptions().get( i );
      parameters[i] = transMeta.environmentSubstitute( option.getParameter() );
      values[i] = transMeta.environmentSubstitute( option.getValue() );
      types[i] = option.getType()==null ? ConfigOption.Type.String.name() : option.getType().name();
    }

    BeamKafkaInputTransform beamInputTransform = new BeamKafkaInputTransform(
      stepMeta.getName(),
      stepMeta.getName(),
      transMeta.environmentSubstitute( beamConsumeMeta.getBootstrapServers() ),
      transMeta.environmentSubstitute( beamConsumeMeta.getTopics() ),
      transMeta.environmentSubstitute( beamConsumeMeta.getGroupId() ),
      beamConsumeMeta.isUsingProcessingTime(),
      beamConsumeMeta.isUsingLogAppendTime(),
      beamConsumeMeta.isUsingCreateTime(),
      beamConsumeMeta.isRestrictedToCommitted(),
      beamConsumeMeta.isAllowingCommitOnConsumedOffset(),
      parameters,
      values,
      types,
      JsonRowMeta.toJson( outputRowMeta ),
      stepPluginClasses,
      xpPluginClasses
    );
    PCollection<KettleRow> afterInput = pipeline.apply( beamInputTransform );
    stepCollectionMap.put( stepMeta.getName(), afterInput );
    log.logBasic( "Handled step (KAFKA INPUT) : " + stepMeta.getName() );
  }
}
