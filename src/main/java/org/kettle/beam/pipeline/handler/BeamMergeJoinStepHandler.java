package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.AssemblerFn;
import org.kettle.beam.core.fn.KettleKeyValueFn;
import org.kettle.beam.core.transform.GroupByTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.groupby.GroupByMeta;
import org.pentaho.di.trans.steps.memgroupby.MemoryGroupByMeta;
import org.pentaho.di.trans.steps.mergejoin.MergeJoinMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BeamMergeJoinStepHandler implements BeamStepHandler {

  private IMetaStore metaStore;
  private TransMeta transMeta;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  public BeamMergeJoinStepHandler( IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
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

    MergeJoinMeta meta = (MergeJoinMeta) stepMeta.getStepMetaInterface();

    String joinType = meta.getJoinType();
    String[] leftKeys = meta.getKeyFields1();
    String[] rightKeys = meta.getKeyFields2();

    StepMeta leftInfoStep = meta.getStepIOMeta().getInfoStreams().get( 0 ).getStepMeta();
    if ( leftInfoStep == null ) {
      throw new KettleException( "The left source step isn't defined in the Merge Join step called '" + stepMeta.getName() + "'" );
    }
    PCollection<KettleRow> leftPCollection = stepCollectionMap.get( leftInfoStep.getName() );
    if ( leftPCollection == null ) {
      throw new KettleException( "The left source collection in the pipeline couldn't be found (probably a programming error)" );
    }
    RowMetaInterface leftRowMeta = transMeta.getStepFields( leftInfoStep );

    StepMeta rightInfoStep = meta.getStepIOMeta().getInfoStreams().get( 1 ).getStepMeta();
    if ( rightInfoStep == null ) {
      throw new KettleException( "The right source step isn't defined in the Merge Join step called '" + stepMeta.getName() + "'" );
    }
    PCollection<KettleRow> rightPCollection = stepCollectionMap.get( rightInfoStep.getName() );
    if ( rightPCollection == null ) {
      throw new KettleException( "The right source collection in the pipeline couldn't be found (probably a programming error)" );
    }
    RowMetaInterface rightRowMeta = transMeta.getStepFields( rightInfoStep );

    // Create key-value pairs (KV) for the left collections
    //
    List<String> leftK = new ArrayList<>( Arrays.asList( leftKeys ) );
    RowMetaInterface leftKRowMeta = new RowMeta();
    List<String> leftV = new ArrayList<>();
    RowMetaInterface leftVRowMeta = new RowMeta();
    for ( String leftKey : leftKeys ) {
      leftKRowMeta.addValueMeta( leftRowMeta.searchValueMeta( leftKey ).clone() );
    }
    for ( ValueMetaInterface valueMeta : leftRowMeta.getValueMetaList() ) {
      String valueName = valueMeta.getName();
      if ( Const.indexOfString( valueName, leftKeys ) < 0 ) {
        leftV.add( valueName );
        leftVRowMeta.addValueMeta( valueMeta.clone() );
      }
    }

    KettleKeyValueFn leftKVFn = new KettleKeyValueFn(
      JsonRowMeta.toJson( leftRowMeta ), stepPluginClasses, xpPluginClasses, leftK.toArray( new String[ 0 ] ), leftV.toArray( new String[ 0 ] ), stepMeta.getName() );
    PCollection<KV<KettleRow, KettleRow>> leftKVPCollection = leftPCollection.apply( ParDo.of( leftKVFn ) );

    // Create key-value pairs (KV) for the left collections
    //
    List<String> rightK = new ArrayList<>( Arrays.asList( rightKeys ) );
    RowMetaInterface rightKRowMeta = new RowMeta();
    List<String> rightV = new ArrayList<>();
    RowMetaInterface rightVRowMeta = new RowMeta();
    for ( String rightKey : rightKeys ) {
      rightKRowMeta.addValueMeta( rightRowMeta.searchValueMeta( rightKey ).clone() );
    }
    for ( ValueMetaInterface valueMeta : rightRowMeta.getValueMetaList() ) {
      String valueName = valueMeta.getName();
      if ( Const.indexOfString( valueName, rightKeys ) < 0 ) {
        rightV.add( valueName );
        rightVRowMeta.addValueMeta( valueMeta.clone() );
      }
    }

    KettleKeyValueFn rightKVFn = new KettleKeyValueFn(
      JsonRowMeta.toJson( rightRowMeta ), stepPluginClasses, xpPluginClasses, rightK.toArray( new String[ 0 ] ), rightV.toArray( new String[ 0 ] ), stepMeta.getName() );
    PCollection<KV<KettleRow, KettleRow>> rightKVPCollection = rightPCollection.apply( ParDo.of( rightKVFn ) );

    PCollection<KV<KettleRow, KV<KettleRow, KettleRow>>> kvpCollection;

    Object[] leftNull = RowDataUtil.allocateRowData( leftVRowMeta.size() );
    Object[] rightNull = RowDataUtil.allocateRowData( rightVRowMeta.size() );

    if ( MergeJoinMeta.join_types[ 0 ].equals( joinType ) ) {
      // Inner Join
      //
      kvpCollection = Join.innerJoin( leftKVPCollection, rightKVPCollection );
    } else if ( MergeJoinMeta.join_types[ 1 ].equals( joinType ) ) {
      // Left outer join
      //
      kvpCollection = Join.leftOuterJoin( leftKVPCollection, rightKVPCollection, new KettleRow( rightNull ) );
    } else if ( MergeJoinMeta.join_types[ 2 ].equals( joinType ) ) {
      // Right outer join
      //
      kvpCollection = Join.rightOuterJoin( leftKVPCollection, rightKVPCollection, new KettleRow( leftNull ) );
    } else if ( MergeJoinMeta.join_types[ 3 ].equals( joinType ) ) {
      // Full outer join
      //
      kvpCollection = Join.fullOuterJoin( leftKVPCollection, rightKVPCollection, new KettleRow( leftNull ), new KettleRow( rightNull ) );
    } else {
      throw new KettleException( "Join type '" + joinType + "' is not recognized or supported" );
    }

    // This is the output of the step, we'll try to mimic this
    //
    final RowMetaInterface outputRowMeta = leftVRowMeta.clone();
    outputRowMeta.addRowMeta( leftKRowMeta );
    outputRowMeta.addRowMeta( rightKRowMeta );
    outputRowMeta.addRowMeta( rightVRowMeta );

    // Now we need to collapse the results where we have a Key-Value pair of
    // The key (left or right depending but the same row metadata (leftKRowMeta == rightKRowMeta)
    //    The key is simply a KettleRow
    // The value:
    //    The value is the resulting combination of the Value parts of the left and right side.
    //    These can be null depending on the join type
    // So we want to grab all this information and put it back together on a single row.
    //
    DoFn<KV<KettleRow, KV<KettleRow, KettleRow>>, KettleRow> assemblerFn = new AssemblerFn(
      JsonRowMeta.toJson( outputRowMeta ),
      JsonRowMeta.toJson( leftKRowMeta ),
      JsonRowMeta.toJson( leftVRowMeta ),
      JsonRowMeta.toJson( rightVRowMeta ),
      stepMeta.getName(),
      stepPluginClasses,
      xpPluginClasses
    );

    // Apply the step transform to the previous io step PCollection(s)
    //
    PCollection<KettleRow> stepPCollection = kvpCollection.apply( ParDo.of( assemblerFn ) );

    // Save this in the map
    //
    stepCollectionMap.put( stepMeta.getName(), stepPCollection );

    log.logBasic( "Handled Merge Join (STEP) : " + stepMeta.getName() );
  }
}
