package org.kettle.beam.core.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.GroupByFn;
import org.kettle.beam.core.fn.KettleKeyValueFn;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GroupByTransform extends PTransform<PCollection<KettleRow>, PCollection<KettleRow>> {

  // The non-transient methods are serializing
  // Keep them simple to stay out of trouble.
  //
  private String stepname;
  private String rowMetaJson;   // The input row
  private String[] groupFields;  // The fields to group over
  private String[] subjects; // The subjects to aggregate on
  private String[] aggregations; // The aggregation types
  private String[] resultFields; // The result fields
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private static final Logger LOG = LoggerFactory.getLogger( GroupByTransform.class );
  private final Counter numErrors = Metrics.counter( "main", "GroupByTransformErrors" );

  private transient RowMetaInterface inputRowMeta;
  private transient RowMetaInterface groupRowMeta;
  private transient RowMetaInterface subjectRowMeta;

  public GroupByTransform() {
  }

  public GroupByTransform( String stepname, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses, String[] groupFields, String[] subjects, String[] aggregations, String[] resultFields) {
    this.stepname = stepname;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.groupFields = groupFields;
    this.subjects = subjects;
    this.aggregations = aggregations;
    this.resultFields = resultFields;
  }

  @Override public PCollection<KettleRow> expand( PCollection<KettleRow> input ) {
    try {
      if ( inputRowMeta == null ) {
        BeamKettle.init(stepPluginClasses, xpPluginClasses);

        inputRowMeta = JsonRowMeta.fromJson( rowMetaJson );

        groupRowMeta = new RowMeta();
        for (int i=0;i<groupFields.length;i++) {
          groupRowMeta.addValueMeta( inputRowMeta.searchValueMeta( groupFields[i] ) );
        }
        subjectRowMeta = new RowMeta();
        for (int i=0;i<subjects.length;i++) {
          subjectRowMeta.addValueMeta( inputRowMeta.searchValueMeta( subjects[i] ) );
        }
      }

      // Split the KettleRow into GroupFields-KettleRow and SubjectFields-KettleRow
      //
      PCollection<KV<KettleRow, KettleRow>> groupSubjects = input.apply( ParDo.of(
        new KettleKeyValueFn( rowMetaJson, stepPluginClasses, xpPluginClasses, groupFields, subjects, stepname )
      ) );

      // Now we need to aggregate the groups with a Combine
      GroupByKey<KettleRow, KettleRow> byKey = GroupByKey.<KettleRow, KettleRow>create();
      PCollection<KV<KettleRow, Iterable<KettleRow>>> grouped = groupSubjects.apply( byKey );

      // Aggregate the rows in the grouped PCollection
      //   Input: KV<KettleRow>, Iterable<KettleRow>>
      //   This means that The group rows is in KettleRow.  For every one of these, you get a list of subject rows.
      //   We need to calculate the aggregation of these subject lists
      //   Then we output group values with result values behind it.
      //
      String counterName = stepname+" AGG";
      PCollection<KettleRow> output = grouped.apply( ParDo.of(
        new GroupByFn(counterName, JsonRowMeta.toJson(groupRowMeta), stepPluginClasses, xpPluginClasses,
          JsonRowMeta.toJson(subjectRowMeta), aggregations ) ) );

      return output;
    } catch(Exception e) {
      numErrors.inc();
      LOG.error( "Error in group by transform", e );
      throw new RuntimeException( "Error in group by transform", e );
    }
  }


  /**
   * Gets inputRowMetaJson
   *
   * @return value of inputRowMetaJson
   */
  public String getRowMetaJson() {
    return rowMetaJson;
  }

  /**
   * @param rowMetaJson The inputRowMetaJson to set
   */
  public void setRowMetaJson( String rowMetaJson ) {
    this.rowMetaJson = rowMetaJson;
  }

  /**
   * Gets groupFields
   *
   * @return value of groupFields
   */
  public String[] getGroupFields() {
    return groupFields;
  }

  /**
   * @param groupFields The groupFields to set
   */
  public void setGroupFields( String[] groupFields ) {
    this.groupFields = groupFields;
  }

  /**
   * Gets subjects
   *
   * @return value of subjects
   */
  public String[] getSubjects() {
    return subjects;
  }

  /**
   * @param subjects The subjects to set
   */
  public void setSubjects( String[] subjects ) {
    this.subjects = subjects;
  }

  /**
   * Gets aggregations
   *
   * @return value of aggregations
   */
  public String[] getAggregations() {
    return aggregations;
  }

  /**
   * @param aggregations The aggregations to set
   */
  public void setAggregations( String[] aggregations ) {
    this.aggregations = aggregations;
  }

  /**
   * Gets resultFields
   *
   * @return value of resultFields
   */
  public String[] getResultFields() {
    return resultFields;
  }

  /**
   * @param resultFields The resultFields to set
   */
  public void setResultFields( String[] resultFields ) {
    this.resultFields = resultFields;
  }
}
