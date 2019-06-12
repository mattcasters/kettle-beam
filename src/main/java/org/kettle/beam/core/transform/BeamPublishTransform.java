package org.kettle.beam.core.transform;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.PublishMessagesFn;
import org.kettle.beam.core.fn.PublishStringsFn;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BeamPublishTransform extends PTransform<PCollection<KettleRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepname;
  private String topic;
  private String messageType;
  private String messageField;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamPublishTransform.class );

  private transient RowMetaInterface rowMeta;
  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;
  private transient int fieldIndex;

  public BeamPublishTransform() {
  }

  public BeamPublishTransform( String stepname, String topic, String messageType, String messageField, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.stepname = stepname;
    this.topic = topic;
    this.messageType = messageType;
    this.messageField = messageField;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PDone expand( PCollection<KettleRow> input ) {

    try {

      if (rowMeta==null) {
        // Only initialize once on this node/vm
        //
        BeamKettle.init( stepPluginClasses, xpPluginClasses );

        // Inflate the metadata on the node where this is running...
        //
        RowMetaInterface rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        initCounter = Metrics.counter( "init", stepname );
        readCounter = Metrics.counter( "read", stepname );
        outputCounter = Metrics.counter( "output", stepname );
        errorCounter = Metrics.counter( "error", stepname );

        fieldIndex = rowMeta.indexOfValue( messageField );
        if (fieldIndex<0) {
          throw new RuntimeException( "Field '"+messageField+"' couldn't be found in the input row: "+rowMeta.toString() );
        }

        initCounter.inc();
      }

      // String messages...
      //
      if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_STRING.equalsIgnoreCase( messageType ) ) {

        PublishStringsFn stringsFn = new PublishStringsFn( stepname, fieldIndex, rowMetaJson, stepPluginClasses, xpPluginClasses );
        PCollection<String> stringPCollection = input.apply( stepname, ParDo.of(stringsFn) );
        PDone done = PubsubIO.writeStrings().to( topic ).expand( stringPCollection );
        return done;
      }

      // PubsubMessages
      //
      if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_MESSAGE.equalsIgnoreCase( messageType ) ) {
        PublishMessagesFn messagesFn = new PublishMessagesFn( stepname, fieldIndex, rowMetaJson, stepPluginClasses, xpPluginClasses );
        PCollection<PubsubMessage> messagesPCollection = input.apply( ParDo.of( messagesFn ) );
        PDone done = PubsubIO.writeMessages().to( topic ).expand( messagesPCollection );
        return done;
      }

      throw new RuntimeException( "Message type '"+messageType+"' is not yet supported" );

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.error( "Error in beam publish transform", e );
      throw new RuntimeException( "Error in beam publish transform", e );
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

  /**
   * Gets messageField
   *
   * @return value of messageField
   */
  public String getMessageField() {
    return messageField;
  }

  /**
   * @param messageField The messageField to set
   */
  public void setMessageField( String messageField ) {
    this.messageField = messageField;
  }
}
