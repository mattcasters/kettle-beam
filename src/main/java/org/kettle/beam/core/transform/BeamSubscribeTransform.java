package org.kettle.beam.core.transform;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.PubsubMessageToKettleRowFn;
import org.kettle.beam.core.fn.StringToKettleRowFn;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A transform to read data from a Google Cloud Platform PubSub topic
 */
public class BeamSubscribeTransform extends PTransform<PBegin, PCollection<KettleRow>> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepname;
  private String subscription;
  private String topic;
  private String messageType;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamSubscribeTransform.class );

  private transient RowMetaInterface rowMeta;
  private transient Counter initCounter;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  public BeamSubscribeTransform() {
  }

  public BeamSubscribeTransform( @Nullable String name, String stepname, String subscription, String topic, String messageType, String rowMetaJson, List<String> stepPluginClasses,
                                 List<String> xpPluginClasses ) {
    super( name );
    this.stepname = stepname;
    this.subscription = subscription;
    this.topic = topic;
    this.messageType = messageType;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override
  public PCollection<KettleRow> expand( PBegin input ) {

    try {
      if ( rowMeta == null ) {
        // Only initialize once on this node/vm
        //
        BeamKettle.init( stepPluginClasses, xpPluginClasses );

        rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        inputCounter = Metrics.counter( "input", stepname );
        writtenCounter = Metrics.counter( "written", stepname );

        Metrics.counter( "init", stepname ).inc();
      }

      // This stuff only outputs a single field.
      // It's either a Serializable or a String
      //
      PCollection<KettleRow> output;

      if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_STRING.equalsIgnoreCase( messageType ) ) {

        PubsubIO.Read<String> stringRead = PubsubIO.readStrings();
        if ( StringUtils.isNotEmpty(subscription)) {
          stringRead = stringRead.fromSubscription( subscription );
        } else {
          stringRead = stringRead.fromTopic( topic);
        }
        PCollection<String> stringPCollection = stringRead.expand( input );
        output = stringPCollection.apply( stepname, ParDo.of(
          new StringToKettleRowFn( stepname, rowMetaJson, stepPluginClasses, xpPluginClasses )
        ) );

      } else if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_MESSAGE.equalsIgnoreCase( messageType ) ) {

        PubsubIO.Read<PubsubMessage> messageRead = PubsubIO.readMessages();
        if (StringUtils.isNotEmpty( subscription )) {
          messageRead = messageRead.fromSubscription( subscription );
        } else {
          messageRead = messageRead.fromTopic( topic );
        }
        PCollection<PubsubMessage> messagesPCollection = messageRead.expand( input );
        output = messagesPCollection.apply( stepname, ParDo.of(
          new PubsubMessageToKettleRowFn( stepname, rowMetaJson, stepPluginClasses, xpPluginClasses )
        ) );

      } else {
        throw new RuntimeException( "Unsupported message type: " + messageType );
      }

      return output;
    } catch ( Exception e ) {
      Metrics.counter( "error", stepname ).inc();
      LOG.error( "Error in beam subscribe transform", e );
      throw new RuntimeException( "Error in beam subscribe transform", e );
    }
  }

  /**
   * Gets stepname
   *
   * @return value of stepname
   */
  public String getStepname() {
    return stepname;
  }

  /**
   * @param stepname The stepname to set
   */
  public void setStepname( String stepname ) {
    this.stepname = stepname;
  }

  /**
   * Gets topic
   *
   * @return value of topic
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic The topic to set
   */
  public void setTopic( String topic ) {
    this.topic = topic;
  }

  /**
   * Gets messageType
   *
   * @return value of messageType
   */
  public String getMessageType() {
    return messageType;
  }

  /**
   * @param messageType The messageType to set
   */
  public void setMessageType( String messageType ) {
    this.messageType = messageType;
  }
}
