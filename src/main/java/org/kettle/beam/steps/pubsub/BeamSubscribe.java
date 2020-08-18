package org.kettle.beam.steps.pubsub;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.kettle.beam.core.BeamDefaults;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaSerializable;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Classe respnsável por execução do step
 * Pub/Sub Subscriber.
 *
 * @author Renato Dornelas Cardoso <renato@romaconsulting.com.br>
 */
public class BeamSubscribe extends BaseStep implements StepInterface {


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
  public BeamSubscribe( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    BeamSubscribeMeta meta = (BeamSubscribeMeta)smi;
    BeamSubscribeData data = (BeamSubscribeData)sdi;

    RowMeta outputRowMeta = new RowMeta();
    String type = this.getParentVariableSpace().environmentSubstitute(meta.getMessageType());
    String fieldName = this.getParentVariableSpace().environmentSubstitute(meta.getMessageField());
    if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_STRING.equalsIgnoreCase(type) ) {
      outputRowMeta.addValueMeta(new ValueMetaString(fieldName));
    } else {
      outputRowMeta.addValueMeta(new ValueMetaSerializable(fieldName));
    }

    MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
              this.flush(outputRowMeta, type, message);
              consumer.ack();
    };

    FlowControlSettings flowControlSettings = FlowControlSettings.newBuilder()
                    .setMaxOutstandingElementCount(1000L)
                    .setMaxOutstandingRequestBytes(100L * 1024L * 1024L)
                    .build();

    Subscriber subscriber = null;

    try {
      subscriber = Subscriber.newBuilder(meta.getSubscription(), receiver)
              .setFlowControlSettings(flowControlSettings)
              .build();
      subscriber.startAsync().awaitRunning();
      System.out.printf("Listening for messages on %s:\n", meta.getSubscription());
      subscriber.awaitTerminated(30, TimeUnit.SECONDS);

    } catch (TimeoutException timeoutException) {
      subscriber.stopAsync();

    }

    return true;
  }

  private void flush(RowMeta outputRowMeta, String type, PubsubMessage message) {
    try{
      Object[] newRow;
      if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_STRING.equalsIgnoreCase(type) ) {
        newRow = new Object[] { message.getData().toStringUtf8() };
      } else {
        newRow = new Object[] { message };
      }
      this.putRow(outputRowMeta, newRow);
      if (isRowLevel()) {
        logRowlevel("Google Pub/Sub -> Subscriber", outputRowMeta.getString(newRow));
      }
    }catch (Exception ex){
      this.log.logError("Google Pub/Sub -> Subscriber", ex);
    }
  }

}
