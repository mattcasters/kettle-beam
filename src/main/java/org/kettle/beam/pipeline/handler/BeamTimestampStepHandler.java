package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.TimestampFn;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.steps.window.BeamTimestampMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamTimestampStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamTimestampStepHandler( BeamJobConfig beamJobConfig, IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( beamJobConfig, false, false, metaStore, transMeta, stepPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input ) throws KettleException {

    BeamTimestampMeta beamTimestampMeta = (BeamTimestampMeta) stepMeta.getStepMetaInterface();

    if ( !beamTimestampMeta.isReadingTimestamp() && StringUtils.isNotEmpty( beamTimestampMeta.getFieldName() ) ) {
      if ( rowMeta.searchValueMeta( beamTimestampMeta.getFieldName() ) == null ) {
        throw new KettleException( "Please specify a valid field name '" + stepMeta.getName() + "'" );
      }
    }

    PCollection<KettleRow> stepPCollection = input.apply( ParDo.of(
      new TimestampFn(
        stepMeta.getName(),
        JsonRowMeta.toJson( rowMeta ),
        transMeta.environmentSubstitute( beamTimestampMeta.getFieldName() ),
        beamTimestampMeta.isReadingTimestamp(),
        stepPluginClasses,
        xpPluginClasses
      ) ) );


    // Save this in the map
    //
    stepCollectionMap.put( stepMeta.getName(), stepPCollection );
    log.logBasic( "Handled step (TIMESTAMP) : " + stepMeta.getName() + ", gets data from " + previousSteps.size() + " previous step(s)" );
  }
}
