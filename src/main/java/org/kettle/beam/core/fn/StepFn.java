package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.injector.InjectorMeta;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StepFn extends DoFn<KettleRow, KettleRow> {

  public static final String INJECTOR_STEP_NAME = "_INJECTOR_";
  private String stepname;
  private String stepPluginId;
  private String stepMetaInterfaceXml;
  private String rowMetaXml;

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

  private transient StepInterface injectorStepInterface;
  private transient StepMetaInterface injectorMetaInterface;
  private transient StepDataInterface injectorDataInterface;

  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter writtenCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( StepFn.class );
  private final Counter numErrors = Metrics.counter( "main", "StepProcessErrors" );

  public StepFn() {
  }

  public StepFn( String stepname, String stepPluginId, String stepMetaInterfaceXml, String inputRowMetaXml) throws KettleException, IOException {
    this.stepname = stepname;
    this.stepPluginId = stepPluginId;
    this.stepMetaInterfaceXml = stepMetaInterfaceXml;
    this.rowMetaXml = inputRowMetaXml;
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      if ( transMeta ==null) {

        // Just to make sure
        BeamKettle.init();

        // Create a very simple new transformation to run single threaded...
        // Single threaded...
        //
        transMeta = new TransMeta();
        transMeta.setTransformationType( TransMeta.TransformationType.SingleThreaded );

        // Input row metadata...
        //
        inputRowMeta = new RowMeta(XMLHandler.getSubNode( XMLHandler.loadXMLString( rowMetaXml), RowMeta.XML_META_TAG ));


        // Create an Injector step with the right row layout...
        // This will help all steps see the row layout statically...
        //
        InjectorMeta injectorMeta = new InjectorMeta();
        injectorMeta.allocate( inputRowMeta.size() );
        for (int i=0;i<inputRowMeta.size();i++) {
          ValueMetaInterface valueMeta = inputRowMeta.getValueMeta( i );
          injectorMeta.getFieldname()[i] = valueMeta.getName();
          injectorMeta.getType()[i] = valueMeta.getType();
          injectorMeta.getLength()[i] = valueMeta.getLength();
          injectorMeta.getPrecision()[i] = valueMeta.getPrecision();
        }
        StepMeta injectorStepMeta = new StepMeta( INJECTOR_STEP_NAME, injectorMeta);
        transMeta.addStep(injectorStepMeta);

        // The step metadata without the wrappers...
        //
        PluginRegistry registry = PluginRegistry.getInstance();
        stepMetaInterface = registry.loadClass( StepPluginType.class, stepPluginId, StepMetaInterface.class );
        stepMetaInterface.loadXML( XMLHandler.getSubNode(XMLHandler.loadXMLString( stepMetaInterfaceXml ), StepMeta.XML_TAG), new ArrayList<>(), new MemoryMetaStore() ) ;
        stepMeta = new StepMeta(stepname, stepMetaInterface);
        stepMeta.setStepID( stepPluginId );
        transMeta.addStep( stepMeta );
        transMeta.addTransHop(new TransHopMeta( injectorStepMeta, stepMeta ) );

        // Create the transformation...
        //
        trans = new Trans( transMeta );
        trans.setLogLevel( LogLevel.ERROR );
        trans.prepareExecution( null );

        rowProducer = trans.addRowProducer( INJECTOR_STEP_NAME, 0 );

        resultRows = new ArrayList<>();

        // Find the right combi
        //
        for ( StepMetaDataCombi combi : trans.getSteps()) {
          if (stepname.equalsIgnoreCase( combi.stepname )) {
            if (stepname.equalsIgnoreCase( "Only CA" )) {
              System.out.print( "...." );
            }
            stepInterface = combi.step;
            stepDataInterface = combi.data;

            ((BaseStep)stepInterface).setUsingThreadPriorityManagment( false );

            rowListener = new RowAdapter() {
              @Override public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
                resultRows.add(row);
              }
            };
            stepInterface.addRowListener( rowListener );
            break;
          }
          if (combi.stepname.equals(INJECTOR_STEP_NAME)) {
            injectorStepInterface = combi.step;
            injectorMetaInterface = combi.meta;
            injectorDataInterface = combi.data;
          }
        }

        if (this.injectorStepInterface==null) {
          throw new KettleException( "Unable to find injector step '"+INJECTOR_STEP_NAME+"' in transformation" );
        }
        if (this.stepInterface==null) {
          throw new KettleException( "Unable to find step '"+stepname+"' in transformation" );
        }

        // Initialize the step as well...
        //
        boolean ok = injectorStepInterface.init( injectorMetaInterface, injectorDataInterface);
        if ( !ok ) {
          throw new KettleException( "Unable to initialize step '" + INJECTOR_STEP_NAME + "'" );
        }
        ok = stepInterface.init( this.stepMetaInterface, stepDataInterface );
        if ( !ok ) {
          throw new KettleException( "Unable to initialize step '" + stepname + "'" );
        }

        initCounter = Metrics.counter( "init", stepname);
        readCounter = Metrics.counter( "read", stepname);
        writtenCounter = Metrics.counter( "written", stepname);

        // Doesn't really start the threads in single threaded mode
        // Just sets some flags all over the place
        //
        trans.startThreads();

        initCounter.inc();
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
      injectorStepInterface.processRow( injectorMetaInterface, injectorDataInterface );
      stepInterface.processRow( stepMetaInterface, stepDataInterface );

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
