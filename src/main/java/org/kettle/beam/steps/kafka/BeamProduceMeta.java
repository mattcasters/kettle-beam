package org.kettle.beam.steps.kafka;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
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
import org.pentaho.di.trans.steps.dummytrans.DummyTransData;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@Step(
  id = "BeamKafkaProduce",
  name = "Beam Kafka Produce",
  description = "Send messages to a Kafka Topic (Producer)",
  image = "beam-kafka-output.svg",
  categoryDescription = "Big Data"
)
public class BeamProduceMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String BOOTSTRAP_SERVERS = "bootstrap_servers";
  public static final String TOPIC = "topic";
  public static final String KEY_FIELD = "key_field";
  public static final String MESSAGE_FIELD = "message_field";

  private String bootstrapServers;
  private String topic;
  private String keyField;
  private String messageField;

  public BeamProduceMeta() {
    super();
  }

  @Override public void setDefault() {
    bootstrapServers = "bootstrapServer1:9001,bootstrapServer2:9001";
    topic = "Topic1";
    keyField = "";
    messageField = "";
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new BeamConsume( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new DummyTransData();
  }

  @Override public String getDialogClassName() {
    return BeamProduceDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {

    // No output
    //
    inputRowMeta.clear();
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( BOOTSTRAP_SERVERS, bootstrapServers ) );
    xml.append( XMLHandler.addTagValue( TOPIC, topic ) );
    xml.append( XMLHandler.addTagValue( KEY_FIELD, keyField ) );
    xml.append( XMLHandler.addTagValue( MESSAGE_FIELD, messageField ) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    bootstrapServers = XMLHandler.getTagValue( stepnode, BOOTSTRAP_SERVERS );
    topic = XMLHandler.getTagValue( stepnode, TOPIC );
    keyField = XMLHandler.getTagValue( stepnode, KEY_FIELD );
    messageField = XMLHandler.getTagValue( stepnode, MESSAGE_FIELD );
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
   * Gets keyField
   *
   * @return value of keyField
   */
  public String getKeyField() {
    return keyField;
  }

  /**
   * @param keyField The keyField to set
   */
  public void setKeyField( String keyField ) {
    this.keyField = keyField;
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

  /**
   * Gets bootstrapServers
   *
   * @return value of bootstrapServers
   */
  public String getBootstrapServers() {
    return bootstrapServers;
  }

  /**
   * @param bootstrapServers The bootstrapServers to set
   */
  public void setBootstrapServers( String bootstrapServers ) {
    this.bootstrapServers = bootstrapServers;
  }
}
