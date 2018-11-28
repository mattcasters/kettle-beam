package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.QueueRowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class StepFn extends DoFn<KettleRow, KettleRow> {

  private String transMetaXml;
  private String stepname;

  private transient TransMeta transMeta = null;
  private transient StepMeta stepMeta = null;
  private transient RowMetaInterface inputRowMeta;
  private transient StepInterface stepInterface;
  private transient StepMetaInterface stepMetaInterface;
  private transient StepDataInterface stepDataInterface;
  private transient QueueRowSet inputRowSet;
  private transient QueueRowSet outputRowSet;
  private transient Trans trans;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( StepFn.class );
  private final Counter numErrors = Metrics.counter( "main", "StepErrors" );

  public StepFn() {
  }

  public StepFn( TransMeta transMeta, String stepname) throws KettleException {
    this.transMetaXml = transMeta.getXML();
    this.stepname = stepname;

    transMeta = null;
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      if (transMeta==null) {

        // Inflate step metadata from XML
        //
        transMeta = new TransMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( transMetaXml ), TransMeta.XML_TAG ), null );
        stepMeta = transMeta.findStep( stepname );
        inputRowMeta = transMeta.getPrevStepFields( stepMeta );

        trans = new Trans( transMeta );
        trans.prepareExecution( null );

        this.stepMetaInterface = stepMeta.getStepMetaInterface();
        this.stepDataInterface = stepMetaInterface.getStepData();
        this.stepInterface = trans.findStepInterface( stepname, 0 );

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
        if ( !ok ) {
          throw new KettleException( "Unable to initialize step '" + stepname + "'" );
        }
      }

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
