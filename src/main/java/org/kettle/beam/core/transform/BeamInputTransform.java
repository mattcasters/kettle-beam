package org.kettle.beam.core.transform;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.StringToKettleFn;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.beaminput.BeamInputMeta;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class BeamInputTransform extends PTransform<PBegin, PCollection<KettleRow>> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepMetaXml;
  private FileDefinition fileDefinition;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamInputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamInputError" );

  public BeamInputTransform( StepMeta stepMeta, FileDefinition fileDefinition ) throws KettleException {
    this.stepMetaXml = stepMeta.getXML();
    this.fileDefinition = fileDefinition;
  }

  @Override public PCollection<KettleRow> expand( PBegin input ) {

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
      StepMeta stepMeta = new StepMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( stepMetaXml ), StepMeta.XML_TAG ), new ArrayList<>(), new MemoryMetaStore() );
      BeamInputMeta beamInputMeta = (BeamInputMeta) stepMeta.getStepMetaInterface();

      return input

        // We read a bunch of Strings, one per line basically
        //
        .apply( stepMeta.getName() + " READ FILE", TextIO.read().from( beamInputMeta.getInputLocation() ) )

        // We need to transform these lines into Kettle fields
        //
        .apply( ParDo.of( new StringToKettleFn( fileDefinition ) ) )

        // From here on out the pipeline contains KettleRow
        //
        ;

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in beam input transform", e );
      return null;
    }

  }
}
