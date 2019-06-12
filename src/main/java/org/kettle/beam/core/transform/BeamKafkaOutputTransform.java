package org.kettle.beam.core.transform;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.KettleRowToKVStringStringFn;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BeamKafkaOutputTransform extends PTransform<PCollection<KettleRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepname;
  private String bootstrapServers;
  private String topic;
  private String keyField;
  private String messageField;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamKafkaOutputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamKafkaOutputError" );

  public BeamKafkaOutputTransform() {
  }

  public BeamKafkaOutputTransform( String stepname, String bootstrapServers, String topic, String keyField, String messageField, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.stepname = stepname;
    this.bootstrapServers = bootstrapServers;
    this.topic = topic;
    this.keyField = keyField;
    this.messageField = messageField;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PDone expand( PCollection<KettleRow> input ) {

    try {
      // Only initialize once on this node/vm
      //
      BeamKettle.init( stepPluginClasses, xpPluginClasses );

      // Inflate the metadata on the node where this is running...
      //
      RowMetaInterface rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      int keyIndex = rowMeta.indexOfValue( keyField );
      if (keyIndex<0) {
        throw new KettleException( "Unable to find key field "+keyField+" in input row: "+rowMeta.toString() );
      }
      int messageIndex = rowMeta.indexOfValue( messageField );
      if (messageIndex<0) {
        throw new KettleException( "Unable to find message field "+messageField+" in input row: "+rowMeta.toString() );
      }

      // First convert the input stream of KettleRows to KV<String,String> for the keys and messages
      //
      KettleRowToKVStringStringFn kettleRowToKVStringStringFn = new KettleRowToKVStringStringFn( stepname, keyIndex, messageIndex, rowMetaJson, stepPluginClasses, xpPluginClasses );

      // Then write to Kafka topic
      //
      KafkaIO.Write<String, String> stringsToKafka = KafkaIO.<String, String>write()
        .withBootstrapServers( bootstrapServers )
        .withTopic( topic )
        .withKeySerializer( StringSerializer.class )
        .withValueSerializer( StringSerializer.class );
      // TODO: add features like compression
      //

      PCollection<KV<String, String>> kvpCollection = input.apply( ParDo.of( kettleRowToKVStringStringFn ) );
      return kvpCollection.apply( stringsToKafka );
    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in Beam Kafka output transform", e );
      throw new RuntimeException( "Error in Beam Kafka output transform", e );
    }
  }
}
