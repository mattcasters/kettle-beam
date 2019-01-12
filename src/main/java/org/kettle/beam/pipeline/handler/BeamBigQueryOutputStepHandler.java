package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamBQOutputTransform;
import org.kettle.beam.core.transform.BeamOutputTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.bq.BeamBQOutputMeta;
import org.kettle.beam.steps.io.BeamOutputMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamBigQueryOutputStepHandler implements BeamStepHandler {

  private IMetaStore metaStore;
  private TransMeta transMeta;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  public BeamBigQueryOutputStepHandler( IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
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

  @Override public void handleStep( LogChannelInterface log, StepMeta beamOutputStepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input  ) throws KettleException {

    BeamBQOutputMeta beamOutputMeta = (BeamBQOutputMeta) beamOutputStepMeta.getStepMetaInterface();

    BeamBQOutputTransform beamOutputTransform = new BeamBQOutputTransform(
      beamOutputStepMeta.getName(),
      transMeta.environmentSubstitute( beamOutputMeta.getProjectId() ),
      transMeta.environmentSubstitute( beamOutputMeta.getDatasetId() ),
      transMeta.environmentSubstitute( beamOutputMeta.getTableId() ),
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
    log.logBasic( "Handled step (BQ OUTPUT) : " + beamOutputStepMeta.getName() + ", gets data from " + previousStep.getName() );
  }

  private FileDefinition getDefaultFileDefition( StepMeta beamOutputStepMeta ) throws KettleStepException {
    FileDefinition fileDefinition = new FileDefinition();

    fileDefinition.setName( "Default" );
    fileDefinition.setEnclosure( "\"" );
    fileDefinition.setSeparator( "," );

    return fileDefinition;
  }

  private void addAllFieldsToEmptyFileDefinition( RowMetaInterface rowMeta, FileDefinition fileDefinition ) throws KettleStepException {
    if ( fileDefinition.getFieldDefinitions().isEmpty() ) {
      for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList() ) {
        fileDefinition.getFieldDefinitions().add( new FieldDefinition(
            valueMeta.getName(),
            valueMeta.getTypeDesc(),
            valueMeta.getLength(),
            valueMeta.getPrecision(),
            valueMeta.getConversionMask()
          )
        );
      }
    }
  }

}
