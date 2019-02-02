package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamBQInputTransform;
import org.kettle.beam.core.transform.BeamKafkaInputTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.steps.bq.BeamBQInputMeta;
import org.kettle.beam.steps.kafka.BeamConsumeMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamKafkaInputStepHandler implements BeamStepHandler {

  private IMetaStore metaStore;
  private TransMeta transMeta;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  public BeamKafkaInputStepHandler( IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.metaStore = metaStore;
    this.transMeta = transMeta;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  public boolean isInput() {
    return true;
  }

  public boolean isOutput() {
    return false;
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

    BeamKafkaInputTransform beamInputTransform = new BeamKafkaInputTransform(
      stepMeta.getName(),
      stepMeta.getName(),
      transMeta.environmentSubstitute( beamConsumeMeta.getBootstrapServers() ),
      transMeta.environmentSubstitute( beamConsumeMeta.getTopics() ),
      JsonRowMeta.toJson( outputRowMeta ),
      stepPluginClasses,
      xpPluginClasses
    );
    PCollection<KettleRow> afterInput = pipeline.apply( beamInputTransform );
    stepCollectionMap.put( stepMeta.getName(), afterInput );
    log.logBasic( "Handled step (KAFKA INPUT) : " + stepMeta.getName() );
  }
}
