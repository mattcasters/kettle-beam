package org.kettle.beam.core;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.fn.ProcessRowInTransFn;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.SingleThreadedTransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Transform using a string of steps in a transformation.
 *
 * This transform has an Injector to pass the collection of rows into.
 * It has an output step from which we'll collect rows.
 *
 */
public class TransMetaTransform extends PTransform<PCollection<KettleRow>, PCollection<KettleRow>> {

  // Important: ALL these object need to be Serializable (writeObject, readObject, ...)
  //
  private String transMetaXml;
  private String inputStepName;
  private String outputStepName;

  // Transient fields are not getting serialized
  //
  private transient TransMeta transMeta = null;
  private transient RowMetaInterface rowMeta = null;
  private transient Trans trans = null;
  private transient SingleThreadedTransExecutor executor = null;
  private transient RowProducer rowProducer = null;
  private transient List<Object[]> outputRows = null;

  public TransMetaTransform() {
  }

  public TransMetaTransform( String topicName, TransMeta transMeta, String inputStepName, String outputStepName ) throws KettleException  {
    super( topicName );
    this.transMetaXml = transMeta.getXML();
    this.inputStepName = inputStepName;
    this.outputStepName = outputStepName;
    this.rowMeta = null;
  }

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( TransMetaTransform.class );
  private static final Counter numParseErrors = Metrics.counter( "main", "ParseErrors" );

  @Override public PCollection<KettleRow> expand( PCollection<KettleRow> input ) {

    try {

      if (executor==null) {
        Document transDocument = XMLHandler.loadXMLString( transMetaXml );

        transMeta = new TransMeta( XMLHandler.getSubNode( transDocument, TransMeta.XML_TAG), null );
        rowMeta = transMeta.getStepFields( inputStepName );

        // Create a new Trans object...
        //
        Trans trans = new Trans(transMeta);
        trans.prepareExecution( new String[0] );

        executor = new SingleThreadedTransExecutor( trans );
        rowProducer = trans.addRowProducer( inputStepName, 0 );

        outputRows = new ArrayList<>();

        // Add a step listener...
        //
        StepInterface outputStep = trans.findStepInterface( outputStepName, 0 );
        RowListener outputRowListener = new RowAdapter() {
          @Override public void rowReadEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
            outputRows.add(row);
          }
        };
        outputStep.addRowListener( outputRowListener );
      }

      // Figure out how to pass the collection of rows in input to the row producer
      //
      ProcessRowInTransFn processRowInTransFn = new ProcessRowInTransFn( rowMeta, rowProducer, executor, outputRows );
      input.apply( "Passing rows to transformation", ParDo.of(processRowInTransFn) );


    } catch ( Exception e ) {
      numParseErrors.inc();
      LOG.error("There was an error running transformation snippet '"+transMeta.getName()+"'", e);
    }
    return null;
  }




}
