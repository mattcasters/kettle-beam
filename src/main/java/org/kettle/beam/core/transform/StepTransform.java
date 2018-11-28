package org.kettle.beam.core.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.StepFn;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepTransform  extends PTransform<PCollection<KettleRow>, PCollection<KettleRow>> {

  protected String stepName;
  protected String transMetaXml;
  protected String stepMetaXml;
  protected String rowMetaXml;

  private transient TransMeta transMeta;
  private transient StepMeta stepMeta;
  private transient RowMetaInterface rowMeta;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( StepTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "StepErrors" );


  public StepTransform( TransMeta transMeta, StepMeta stepMeta, RowMetaInterface rowMeta ) throws KettleException {
    try {
      this.stepName = stepMeta.getName();
      this.transMetaXml = transMeta.getXML();
      this.stepMetaXml = stepMeta.getXML();
      this.rowMetaXml = rowMeta.getMetaXML();
    } catch(Exception e) {
      throw new KettleException( "Error serializing step transform metadata to XML", e );
    }
  }

  @Override public PCollection<KettleRow> expand( PCollection<KettleRow> input ) {

    try {

      // Only initialize once on this node/vm
      //
      synchronized ( this ) {
        if ( !KettleEnvironment.isInitialized() ) {
          KettleEnvironment.init();
        }
      }

      // Inflate the metadata on the node where this is running...
      //
      transMeta = new TransMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( transMetaXml ), TransMeta.XML_TAG), null );
      stepMeta = new StepMeta(XMLHandler.getSubNode( XMLHandler.loadXMLString( stepMetaXml ), StepMeta.XML_TAG ), transMeta.getDatabases(), transMeta.getMetaStore() );
      rowMeta = new RowMeta(XMLHandler.getSubNode( XMLHandler.loadXMLString( rowMetaXml), RowMeta.XML_META_TAG ));

      // Create a dummy trans object, dirty but we'll see how far we get...
      //
      Trans trans = new Trans(transMeta);

      // Let's create the Step object to go along with the metadata...
      //
      StepMetaInterface stepMetaInterface = stepMeta.getStepMetaInterface();
      StepDataInterface stepData = stepMetaInterface.getStepData();
      StepInterface stepInterface = stepMetaInterface.getStep( stepMeta, stepData, 0, transMeta, trans );
      BaseStep baseStep = (BaseStep)stepInterface;

      // Create a new step function, initializes the step
      //
      StepFn stepFn = new StepFn( stepName, stepInterface, stepMetaInterface, stepData, rowMeta );

      PCollection<KettleRow> output = input.apply( ParDo.of( stepFn ) );

      return output;

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error("Error transforming data in step '"+stepName+"'", e);
      return null;
    }

  }

}
