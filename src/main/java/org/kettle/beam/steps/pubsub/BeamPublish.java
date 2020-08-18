package org.kettle.beam.steps.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.pubsub.v1.Publisher;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BeamPublish extends BaseStep implements StepInterface {

  //region Attributes

  public static final String GOOGLE_ENVIRONMENT_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS";
  private GoogleCredentials credentials;

  //endregion

  //region Methods

  /**
   * This is the base step that forms that basis for all steps. You can derive from this class to implement your own
   * steps.
   *
   * @param stepMeta          The StepMeta object to run.
   * @param stepDataInterface the data object to store temporary data, database connections, caches, result sets,
   *                          hashtables etc.
   * @param copyNr            The copynumber for this step.
   * @param transMeta         The TransInfo of which the step stepMeta is part of.
   * @param trans             The (running) transformation to obtain information shared among the steps.
   */
  public BeamPublish( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    if(this.getCurrentInputRowSetNr() < this.getRow().length){
      Object objValue = this.getRow()[this.getCurrentInputRowSetNr()];
      String strValue = objValue != null ? objValue.toString() : "null";
      this.log.logDetailed(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> Input: " + strValue);
      BeamPublishMeta metaData = (BeamPublishMeta)this.getStepMeta().getStepMetaInterface();
      this.publish(metaData.getTopic(), strValue);

    }else{
      this.log.logBasic(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> Índice de entrada maior que quantidade de informação.");

    }

    return true;
  }

  private void publish(String topicName, String value) {
    List<ApiFuture<String>> messageIdFutures = new ArrayList<>();
    Publisher publisher = null;
    try {
      ServiceAccountCredentials credentials = (ServiceAccountCredentials)this.getCredentials();
      ProjectTopicName topic = ProjectTopicName.of(credentials.getProjectId(), topicName);
      publisher = Publisher.newBuilder(topicName).setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
      ByteString data = ByteString.copyFromUtf8(value);
      PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
      ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
      messageIdFutures.add(messageIdFuture);
      this.log.logDetailed(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> Publish Success!");

    } catch (Exception ex) {
      this.log.logError(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> Ocorreu um erro quando o software tentou publicar a mensagem no GCP.", ex);

    } finally {
      try {
        List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();
        for (String messageId : messageIds) {
          this.log.logDebug(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> published with message ID: " + messageId);
        }
        if (publisher != null) {
          try {
            publisher.shutdown();
            publisher.awaitTermination(1, TimeUnit.MINUTES);
          } catch (InterruptedException e) {
            this.log.logError(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> Ocorreu um erro ao tentar finalizar o Publish no GCP. Exception : ", e);
          }
        }

      } catch (Exception e) {
        this.log.logError(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> Ocorreu um erro ao tentar obter os ids das mensagens publicadas no GCP. Exception : ", e);
      }
    }

  }


  private GoogleCredentials getCredentials() {
    if(this.credentials != null){return this.credentials;}
    String configPath = System.getenv(GOOGLE_ENVIRONMENT_VARIABLE);
    InputStream inputStream = null;
    GoogleCredentials credentials;
    if(StringUtils.isNotEmpty(configPath)){
      try {
        inputStream = new FileInputStream(configPath);
      }catch (Exception ex){
        inputStream = null;
      }
    }
    if(inputStream == null){
      this.log.logError(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> Erro: Não foi possível carregar o arquivo de credenciais do Google Cloud, favor configurar a variável de ambiente '" + GOOGLE_ENVIRONMENT_VARIABLE + "' apontando para o arquivo JSON com as credenciais.");
    }
    try {
      this.credentials = GoogleCredentials.fromStream(inputStream);
    } catch (Exception e) {
      this.log.logError(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> Erro ao carregar o arquivo de credenciais do Google Cloud.", e);
    }
    return this.credentials;
  }

  //endregion

}
