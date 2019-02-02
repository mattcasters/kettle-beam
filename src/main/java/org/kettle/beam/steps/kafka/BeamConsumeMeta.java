package org.kettle.beam.steps.kafka;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
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
import org.pentaho.di.trans.steps.dummytrans.DummyTransData;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@Step(
  id = "BeamKafkaConsume",
  name = "Beam Kafka Consume",
  description = "Get messages from Kafka topics (Kafka Consumer)",
  image = "beam-kafka-input.svg",
  categoryDescription = "Big Data"
)
public class BeamConsumeMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String BOOTSTRAP_SERVERS = "bootstrap_servers";
  public static final String TOPICS = "topics";
  public static final String KEY_FIELD = "key_field";
  public static final String MESSAGE_FIELD = "message_field";

  private String topics;
  private String bootstrapServers;
  private String keyField;
  private String messageField;

  public BeamConsumeMeta() {
    super();
  }

  @Override public void setDefault() {
    bootstrapServers = "bootstrapServer1:9001,bootstrapServer2:9001";
    topics = "Topic1,Topic2";
    keyField = "key";
    messageField = "message";
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new BeamConsume( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new DummyTransData();
  }

  @Override public String getDialogClassName() {
    return BeamConsumeDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {

    ValueMetaInterface keyValueMeta = new ValueMetaString( space.environmentSubstitute( keyField ) );
    keyValueMeta.setOrigin( name );
    inputRowMeta.addValueMeta( keyValueMeta );

    ValueMetaInterface messageValueMeta = new ValueMetaString( space.environmentSubstitute( messageField ) );
    messageValueMeta.setOrigin( name );
    inputRowMeta.addValueMeta( messageValueMeta );
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( BOOTSTRAP_SERVERS, bootstrapServers ) );
    xml.append( XMLHandler.addTagValue( TOPICS, topics ) );
    xml.append( XMLHandler.addTagValue( KEY_FIELD, keyField ) );
    xml.append( XMLHandler.addTagValue( MESSAGE_FIELD, messageField ) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    bootstrapServers = XMLHandler.getTagValue( stepnode, BOOTSTRAP_SERVERS );
    topics = XMLHandler.getTagValue( stepnode, TOPICS );
    keyField = XMLHandler.getTagValue( stepnode, KEY_FIELD );
    messageField = XMLHandler.getTagValue( stepnode, MESSAGE_FIELD );
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

  /**
   * Gets topics
   *
   * @return value of topics
   */
  public String getTopics() {
    return topics;
  }

  /**
   * @param topics The topics to set
   */
  public void setTopics( String topics ) {
    this.topics = topics;
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
}
