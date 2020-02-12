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

import java.util.ArrayList;
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
  public static final String GROUP_ID = "group_id";
  public static final String USE_PROCESSING_TIME = "use_processing_time";
  public static final String USE_LOG_APPEND_TIME = "use_log_append_time";
  public static final String USE_CREATE_TIME = "use_create_time";
  public static final String RESTRICT_TO_COMMITTED = "restrict_to_committed";
  public static final String ALLOW_COMMIT_ON_CONSUMED = "allow_commit_on_consumed";
  public static final String CONFIG_OPTIONS = "config_options";
  public static final String CONFIG_OPTION = "config_option";
  public static final String CONFIG_OPTION_PARAMETER = "parameter";
  public static final String CONFIG_OPTION_VALUE = "value";
  public static final String CONFIG_OPTION_TYPE = "type";

  private String topics;
  private String bootstrapServers;
  private String keyField;
  private String messageField;
  private String groupId;
  private boolean usingProcessingTime; // default
  private boolean usingLogAppendTime;
  private boolean usingCreateTime;
  private boolean restrictedToCommitted;
  private boolean allowingCommitOnConsumedOffset;
  private List<ConfigOption> configOptions;

  public BeamConsumeMeta() {
    super();
    configOptions = new ArrayList<>();
  }

  @Override public void setDefault() {
    bootstrapServers = "bootstrapServer1:9001,bootstrapServer2:9001";
    topics = "Topic1,Topic2";
    keyField = "key";
    messageField = "message";
    groupId = "GroupID";
    usingProcessingTime = true;
    usingLogAppendTime = false;
    usingCreateTime = false;
    restrictedToCommitted = false;
    allowingCommitOnConsumedOffset = true;
    configOptions = new ArrayList<>();
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
    xml.append( XMLHandler.addTagValue( GROUP_ID, groupId ) );
    xml.append( XMLHandler.addTagValue( USE_PROCESSING_TIME, usingProcessingTime ) );
    xml.append( XMLHandler.addTagValue( USE_LOG_APPEND_TIME, usingLogAppendTime ) );
    xml.append( XMLHandler.addTagValue( USE_CREATE_TIME, usingCreateTime ) );
    xml.append( XMLHandler.addTagValue( RESTRICT_TO_COMMITTED, restrictedToCommitted ) );
    xml.append( XMLHandler.addTagValue( ALLOW_COMMIT_ON_CONSUMED, allowingCommitOnConsumedOffset ) );
    xml.append( XMLHandler.openTag( CONFIG_OPTIONS ));
    for (ConfigOption option :configOptions) {
      xml.append( XMLHandler.openTag( CONFIG_OPTION ));
      xml.append( XMLHandler.addTagValue( CONFIG_OPTION_PARAMETER, option.getParameter() ) );
      xml.append( XMLHandler.addTagValue( CONFIG_OPTION_VALUE, option.getValue() ) );
      xml.append( XMLHandler.addTagValue( CONFIG_OPTION_TYPE, option.getType()==null ? "" : option.getType().name() ) );
      xml.append( XMLHandler.closeTag( CONFIG_OPTION ));
    }
    xml.append( XMLHandler.closeTag( CONFIG_OPTIONS ));
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    bootstrapServers = XMLHandler.getTagValue( stepnode, BOOTSTRAP_SERVERS );
    topics = XMLHandler.getTagValue( stepnode, TOPICS );
    keyField = XMLHandler.getTagValue( stepnode, KEY_FIELD );
    messageField = XMLHandler.getTagValue( stepnode, MESSAGE_FIELD );
    groupId = XMLHandler.getTagValue( stepnode, GROUP_ID );
    usingProcessingTime="Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, USE_PROCESSING_TIME ) );
    usingLogAppendTime="Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, USE_LOG_APPEND_TIME ) );
    usingCreateTime="Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, USE_CREATE_TIME ) );
    restrictedToCommitted="Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, RESTRICT_TO_COMMITTED ) );
    allowingCommitOnConsumedOffset="Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, ALLOW_COMMIT_ON_CONSUMED ) );
    configOptions = new ArrayList<>(  );
    Node optionsNode = XMLHandler.getSubNode( stepnode, CONFIG_OPTIONS );
    List<Node> optionNodes = XMLHandler.getNodes( optionsNode, CONFIG_OPTION );
    for (Node optionNode : optionNodes) {
      String parameter = XMLHandler.getTagValue( optionNode, CONFIG_OPTION_PARAMETER );
      String value = XMLHandler.getTagValue( optionNode, CONFIG_OPTION_VALUE );
      ConfigOption.Type type = ConfigOption.Type.getTypeFromName( XMLHandler.getTagValue( optionNode, CONFIG_OPTION_TYPE ) );
      configOptions.add( new ConfigOption(parameter, value, type));
    }
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

  /**
   * Gets groupId
   *
   * @return value of groupId
   */
  public String getGroupId() {
    return groupId;
  }

  /**
   * @param groupId The groupId to set
   */
  public void setGroupId( String groupId ) {
    this.groupId = groupId;
  }

  /**
   * Gets usingProcessingTime
   *
   * @return value of usingProcessingTime
   */
  public boolean isUsingProcessingTime() {
    return usingProcessingTime;
  }

  /**
   * @param usingProcessingTime The usingProcessingTime to set
   */
  public void setUsingProcessingTime( boolean usingProcessingTime ) {
    this.usingProcessingTime = usingProcessingTime;
  }

  /**
   * Gets usingLogAppendTime
   *
   * @return value of usingLogAppendTime
   */
  public boolean isUsingLogAppendTime() {
    return usingLogAppendTime;
  }

  /**
   * @param usingLogAppendTime The usingLogAppendTime to set
   */
  public void setUsingLogAppendTime( boolean usingLogAppendTime ) {
    this.usingLogAppendTime = usingLogAppendTime;
  }

  /**
   * Gets usingCreateTime
   *
   * @return value of usingCreateTime
   */
  public boolean isUsingCreateTime() {
    return usingCreateTime;
  }

  /**
   * @param usingCreateTime The usingCreateTime to set
   */
  public void setUsingCreateTime( boolean usingCreateTime ) {
    this.usingCreateTime = usingCreateTime;
  }

  /**
   * Gets restrictedToCommitted
   *
   * @return value of restrictedToCommitted
   */
  public boolean isRestrictedToCommitted() {
    return restrictedToCommitted;
  }

  /**
   * @param restrictedToCommitted The restrictedToCommitted to set
   */
  public void setRestrictedToCommitted( boolean restrictedToCommitted ) {
    this.restrictedToCommitted = restrictedToCommitted;
  }

  /**
   * Gets allowingCommitOnConsumedOffset
   *
   * @return value of allowingCommitOnConsumedOffset
   */
  public boolean isAllowingCommitOnConsumedOffset() {
    return allowingCommitOnConsumedOffset;
  }

  /**
   * @param allowingCommitOnConsumedOffset The allowingCommitOnConsumedOffset to set
   */
  public void setAllowingCommitOnConsumedOffset( boolean allowingCommitOnConsumedOffset ) {
    this.allowingCommitOnConsumedOffset = allowingCommitOnConsumedOffset;
  }

  /**
   * Gets configOptions
   *
   * @return value of configOptions
   */
  public List<ConfigOption> getConfigOptions() {
    return configOptions;
  }

  /**
   * @param configOptions The configOptions to set
   */
  public void setConfigOptions( List<ConfigOption> configOptions ) {
    this.configOptions = configOptions;
  }
}
