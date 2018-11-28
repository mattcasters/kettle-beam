package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.QueueRowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepFn extends DoFn<KettleRow, KettleRow> {

  private final String stepName;
  private final RowMetaInterface inputRowMeta;

  private final StepInterface stepInterface;
  private final StepMetaInterface stepMetaInterface;
  private final StepDataInterface stepDataInterface;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( StepFn.class );
  private final Counter numErrors = Metrics.counter( "main", "StepErrors" );
  private final QueueRowSet inputRowSet;
  private final QueueRowSet outputRowSet;

  public StepFn( String stepName, StepInterface stepInterface, StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface, RowMetaInterface inputRowMeta ) throws KettleException {
    this.stepName = stepName;
    this.stepInterface = stepInterface;
    this.inputRowMeta = inputRowMeta;
    this.stepMetaInterface = stepMetaInterface;
    this.stepDataInterface = stepDataInterface;

    // Prepare the step...
    //
    stepInterface.getInputRowSets().clear();
    stepInterface.getOutputRowSets().clear();

    // Handle the input and output rowsets
    //
    inputRowSet = new QueueRowSet();
    outputRowSet = new QueueRowSet();

    stepInterface.getInputRowSets().add( inputRowSet );
    stepInterface.getOutputRowSets().add( outputRowSet );

    // Initialize the step as well...
    //
    boolean ok = stepInterface.init( stepMetaInterface, stepDataInterface );
    if (!ok) {
      throw new KettleException( "Unable to initialize step '"+stepName+"'" );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      // Get one row, pass it through the given stepInterface copy
      // Retrieve the rows and pass them to the processContext
      //
      KettleRow inputRow = processContext.element();

      // Pass the row to the input rowset
      //
      inputRowSet.putRow( inputRowMeta, inputRow.getRow() );

      // Process the row
      //
      stepInterface.processRow( stepMetaInterface, stepDataInterface );

      // Pass all rows in the output to the process context
      //
      Object[] outputRow = outputRowSet.getRow();
      while (outputRow!=null) {

        // Pass the row to the process context
        //
        processContext.output( new KettleRow(outputRow) );
      }

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.info( "Parse error on " + processContext.element() + ", " + e.getMessage() );
    }
  }

  /**
   * Gets inputRowMeta
   *
   * @return value of inputRowMeta
   */
  public RowMetaInterface getInputRowMeta() {
    return inputRowMeta;
  }

  /**
   * Gets stepInterface
   *
   * @return value of stepInterface
   */
  public StepInterface getStepInterface() {
    return stepInterface;
  }

  /**
   * Gets stepMetaInterface
   *
   * @return value of stepMetaInterface
   */
  public StepMetaInterface getStepMetaInterface() {
    return stepMetaInterface;
  }

  /**
   * Gets stepDataInterface
   *
   * @return value of stepDataInterface
   */
  public StepDataInterface getStepDataInterface() {
    return stepDataInterface;
  }
}
