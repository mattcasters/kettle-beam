package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamBQInputTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.steps.bq.BeamBQInputMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamBigQueryInputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamBigQueryInputStepHandler( BeamJobConfig beamJobConfig, IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( beamJobConfig, true, false, metaStore, transMeta, stepPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input ) throws KettleException {

    // Input handling
    //
    BeamBQInputMeta beamInputMeta = (BeamBQInputMeta) stepMeta.getStepMetaInterface();

    // Output rows (fields selection)
    //
    RowMetaInterface outputRowMeta = new RowMeta();
    beamInputMeta.getFields( outputRowMeta, stepMeta.getName(), null, null, transMeta, null, null );

    BeamBQInputTransform beamInputTransform = new BeamBQInputTransform(
      stepMeta.getName(),
      stepMeta.getName(),
      transMeta.environmentSubstitute( beamInputMeta.getProjectId() ),
      transMeta.environmentSubstitute( beamInputMeta.getDatasetId() ),
      transMeta.environmentSubstitute( beamInputMeta.getTableId() ),
      transMeta.environmentSubstitute( beamInputMeta.getQuery() ),
      JsonRowMeta.toJson( outputRowMeta ),
      stepPluginClasses,
      xpPluginClasses
    );
    PCollection<KettleRow> afterInput = pipeline.apply( beamInputTransform );
    stepCollectionMap.put( stepMeta.getName(), afterInput );
    log.logBasic( "Handled step (BQ INPUT) : " + stepMeta.getName() );

  }
}
