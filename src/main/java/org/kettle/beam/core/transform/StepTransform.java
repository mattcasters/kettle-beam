package org.kettle.beam.core.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.StepFn;
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

  protected String stepname;
  protected String transMetaXml;


  private transient TransMeta transMeta;
  private transient StepMeta stepMeta;
  private transient RowMetaInterface rowMeta;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( StepTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "StepErrors" );


  public StepTransform( TransMeta transMeta, String stepname ) throws KettleException {
    try {
      this.transMetaXml = transMeta.getXML();
      this.stepname = stepname;
    } catch(Exception e) {
      throw new KettleException( "Error serializing step transform metadata to XML", e );
    }
  }

  @Override public PCollection<KettleRow> expand( PCollection<KettleRow> input ) {

    try {

      // Only initialize once on this node/vm
      //
      BeamKettle.init();

      // Inflate the metadata on the node where this is running...
      //
      transMeta = new TransMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( transMetaXml ), TransMeta.XML_TAG), null );

      // Create a new step function, initializes the step
      //
      StepFn stepFn = new StepFn( transMeta, stepname );

      PCollection<KettleRow> output = input.apply( ParDo.of( stepFn ) );

      return output;

    } catch ( Exception e ) {
      e.printStackTrace();
      numErrors.inc();
      LOG.error("Error transforming data in step '"+ stepname +"'", e);
      return null;
    }

  }

}
