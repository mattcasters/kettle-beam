package org.kettle.beam.core.transform;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.KVStringStringToKettleRowFn;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class BeamKafkaInputTransform extends PTransform<PBegin, PCollection<KettleRow>> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepname;
  private String bootstrapServers;
  private String topics;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamKafkaInputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamKafkaInputError" );

  private transient RowMetaInterface rowMeta;

  public BeamKafkaInputTransform() {
  }

  public BeamKafkaInputTransform( @Nullable String name, String stepname, String bootstrapServers, String topics, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( name );
    this.stepname = stepname;
    this.bootstrapServers = bootstrapServers;
    this.topics = topics;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PCollection<KettleRow> expand( PBegin input ) {
    try {
      // Only initialize once on this node/vm
      //
      BeamKettle.init( stepPluginClasses, xpPluginClasses );

      // What's the list of topics?
      //
      List<String> topicList = new ArrayList<>();
      for ( String topic : topics.split( "," ) ) {
        topicList.add( Const.trim( topic ) );
      }

      PTransform<PBegin, PCollection<KV<String, String>>> kafkaReadTransform = KafkaIO.<String, String>read()
        .withBootstrapServers( bootstrapServers )
        .withTopics( topicList )
        .withKeyDeserializer( StringDeserializer.class )
        .withValueDeserializer( StringDeserializer.class )
        .withoutMetadata();

      // Read keys and values from Kafka
      //
      PCollection<KV<String, String>> kafkaConsumerOutput = input
        .apply( kafkaReadTransform );

      // Now convert this into Kettle rows with a single String value in them
      //
      PCollection<KettleRow> output = kafkaConsumerOutput.apply( ParDo.of(
        new KVStringStringToKettleRowFn( stepname, rowMetaJson, stepPluginClasses, xpPluginClasses )
      ) );

      return output;

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in Kafka input transform", e );
      throw new RuntimeException( "Error in Kafka input transform", e );
    }
  }
}
