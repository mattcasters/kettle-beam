package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamPublishTransform;
import org.kettle.beam.core.transform.GroupByTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.steps.pubsub.BeamPublishMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.groupby.GroupByMeta;
import org.pentaho.di.trans.steps.memgroupby.MemoryGroupByMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamGroupByStepHandler implements BeamStepHandler {

  private IMetaStore metaStore;
  private TransMeta transMeta;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  public BeamGroupByStepHandler( IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.metaStore = metaStore;
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

    MemoryGroupByMeta groupByMeta = (MemoryGroupByMeta ) stepMeta.getStepMetaInterface();

    String[] aggregates = new String[ groupByMeta.getAggregateType().length ];
    for ( int i = 0; i < aggregates.length; i++ ) {
      aggregates[ i ] = MemoryGroupByMeta.getTypeDesc( groupByMeta.getAggregateType()[ i ] );
    }

    PTransform<PCollection<KettleRow>, PCollection<KettleRow>> stepTransform = new GroupByTransform(
      stepMeta.getName(),
      JsonRowMeta.toJson( rowMeta ),  // The io row
      stepPluginClasses,
      xpPluginClasses,
      groupByMeta.getGroupField(),
      groupByMeta.getSubjectField(),
      aggregates,
      groupByMeta.getAggregateField()
    );

    // Apply the step transform to the previous io step PCollection(s)
    //
    PCollection<KettleRow> stepPCollection = input.apply( stepMeta.getName(), stepTransform );

    // Save this in the map
    //
    stepCollectionMap.put( stepMeta.getName(), stepPCollection );
    log.logBasic( "Handled Group By (STEP) : " + stepMeta.getName() + ", gets data from " + previousSteps.size() + " previous step(s)" );
  }
}
