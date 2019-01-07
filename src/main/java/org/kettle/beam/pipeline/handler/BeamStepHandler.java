package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.StepMeta;

import java.util.List;
import java.util.Map;

public interface BeamStepHandler {

  void handleStep( LogChannelInterface log,
                   StepMeta stepMeta,
                   Map<String, PCollection<KettleRow>> stepCollectionMap,
                   Pipeline pipeline,
                   RowMetaInterface rowMeta,
                   List<StepMeta> previousSteps,
                   PCollection<KettleRow> input
  ) throws KettleException;

  boolean isInput();

  boolean isOutput();
}
