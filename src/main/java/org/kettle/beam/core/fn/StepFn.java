package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.QueueRowSet;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StepFn extends DoFn<KettleRow, KettleRow> {

  private String transMetaXml;
  private String stepname;

  private transient TransMeta transMeta = null;
  private transient StepMeta stepMeta = null;
  private transient RowMetaInterface inputRowMeta;
  private transient StepInterface stepInterface;
  private transient StepMetaInterface stepMetaInterface;
  private transient StepDataInterface stepDataInterface;
  private transient Trans trans;
  private transient RowProducer rowProducer;
  private transient RowListener rowListener;
  private transient List<Object[]> resultRows;

  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter writtenCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( StepFn.class );
  private final Counter numErrors = Metrics.counter( "main", "StepProcessErrors" );

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

        // Just to make sure
        BeamKettle.init();

        // Inflate step metadata from XML
        //
        transMeta = new TransMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( transMetaXml ), TransMeta.XML_TAG ), null );

        // Single threaded...
        //
        transMeta.setTransformationType( TransMeta.TransformationType.SingleThreaded );

        trans = new Trans( transMeta );
        trans.setLogLevel( LogLevel.ERROR );
        trans.prepareExecution( null );


        // Find the right combi
        //
        for ( StepMetaDataCombi combi : trans.getSteps()) {
          if (stepname.equalsIgnoreCase( combi.stepname )) {
            stepInterface = combi.step;
            stepMetaInterface = combi.stepMeta.getStepMetaInterface();
            stepDataInterface = combi.data;
            stepMeta = combi.stepMeta;

            ((BaseStep)stepInterface).setUsingThreadPriorityManagment( false );

            rowProducer = trans.addRowProducer( stepname, 0 );
            resultRows = new ArrayList<>();
            rowListener = new RowAdapter() {
              @Override public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
                resultRows.add(row);
              }
            };
            stepInterface.addRowListener( rowListener );
            break;
          }
        }

        if (this.stepInterface==null) {
          throw new KettleException( "Unable to find step '"+stepname+"' in transformation" );
        }

        inputRowMeta = transMeta.getPrevStepFields( stepMeta );

        // Initialize the step as well...
        //
        boolean ok = stepInterface.init( stepMetaInterface, stepDataInterface );
        if ( !ok ) {
          throw new KettleException( "Unable to initialize step '" + stepname + "'" );
        }

        initCounter = Metrics.counter( "init", stepname);
        readCounter = Metrics.counter( "read", stepname);
        writtenCounter = Metrics.counter( "written", stepname);

        // Doesn't really start the threads in single threaded mode
        // Just sets some flags all over the place
        //
        trans.startThreads();;
      }

      resultRows.clear();

      // Get one row, pass it through the given stepInterface copy
      // Retrieve the rows and pass them to the processContext
      //
      KettleRow inputRow = processContext.element();

      // Pass the row to the input rowset
      //
      rowProducer.putRow( inputRowMeta, inputRow.getRow() );
      readCounter.inc();

      // Process the row
      //
      boolean ok = stepInterface.processRow( stepMetaInterface, stepDataInterface );

      // Pass all rows in the output to the process context
      //
      for (Object[] resultRow : resultRows) {

        // Pass the row to the process context
        //
        processContext.output( new KettleRow(resultRow) );
        writtenCounter.inc();
      }

    } catch ( Exception e ) {
      e.printStackTrace();
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
