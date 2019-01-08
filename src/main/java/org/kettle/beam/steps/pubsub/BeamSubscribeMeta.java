package org.kettle.beam.steps.pubsub;

import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamDefaults;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaSerializable;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step(
  id = "BeamSubscribe",
  name = "Beam GCP Pub/Sub : Subscribe",
  description = "Subscribe to data from a Pub/Sub topic",
  image = "beam-gcp-pubsub-subscribe.svg",
  categoryDescription = "Big Data"
)
public class BeamSubscribeMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String SUBSCRIPTION = "subscription";
  public static final String TOPIC = "topic";
  public static final String MESSAGE_TYPE = "message_type";
  public static final String MESSAGE_FIELD = "message_field";

  private String topic;
  private String subscription;
  private String messageType;
  private String messageField;

  public BeamSubscribeMeta() {
    super();
  }

  @Override public void setDefault() {
    subscription = "Subscription";
    topic = "Topic";
    messageType = "String";
    messageField = "message";
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new BeamSubscribe( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new BeamSubscribeData();
  }

  @Override public String getDialogClassName() {
    return BeamSubscribeDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {

    if ( StringUtils.isEmpty( messageType ) ) {
      throw new KettleStepException( "You need to specify the type of message to read" );
    }
    if ( StringUtils.isEmpty( messageField ) ) {
      throw new KettleStepException( "You need to specify the field name of the message to read" );
    }

    String type = space.environmentSubstitute( messageType );
    String fieldName = space.environmentSubstitute( messageField );

    ValueMetaInterface valueMeta;
    if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_STRING.equalsIgnoreCase( type ) ) {
      valueMeta = new ValueMetaString( fieldName );
    } else {
      valueMeta = new ValueMetaSerializable( fieldName );
    }

    valueMeta.setOrigin( name );
    inputRowMeta.addValueMeta( valueMeta );
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( SUBSCRIPTION, subscription ) );
    xml.append( XMLHandler.addTagValue( TOPIC, topic ) );
    xml.append( XMLHandler.addTagValue( MESSAGE_TYPE, messageType ) );
    xml.append( XMLHandler.addTagValue( MESSAGE_FIELD, messageField ) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    subscription = XMLHandler.getTagValue( stepnode, SUBSCRIPTION );
    topic = XMLHandler.getTagValue( stepnode, TOPIC );
    messageType = XMLHandler.getTagValue( stepnode, MESSAGE_TYPE );
    messageField = XMLHandler.getTagValue( stepnode, MESSAGE_FIELD );
  }


  /**
   * Gets subscription
   *
   * @return value of subscription
   */
  public String getSubscription() {
    return subscription;
  }

  /**
   * @param subscription The subscription to set
   */
  public void setSubscription( String subscription ) {
    this.subscription = subscription;
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
