package org.kettle.beam.core.transform;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.KettleToStringFn;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.beamoutput.BeamOutputMeta;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

public class BeamOutputTransform extends PTransform<PCollection<KettleRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepMetaXml;
  private FileDefinition fileDefinition;
  private String rowMetaXml;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamOutputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamOutputError" );

  public BeamOutputTransform( StepMeta stepMeta, FileDefinition fileDefinition, RowMetaInterface rowMeta ) throws KettleException, IOException {
    this.stepMetaXml = stepMeta.getXML();
    this.fileDefinition = fileDefinition;
    this.rowMetaXml = rowMeta.getMetaXML();
  }

  @Override public PDone expand( PCollection<KettleRow> input ) {

    try {
      // Only initialize once on this node/vm
      //
      BeamKettle.init();

      // Inflate the metadata on the node where this is running...
      //
      RowMeta rowMeta = new RowMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( rowMetaXml ), RowMeta.XML_META_TAG ) );
      StepMeta stepMeta = new StepMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( stepMetaXml ), StepMeta.XML_TAG ), new ArrayList<>(), new MemoryMetaStore() );
      BeamOutputMeta beamOutputMeta = (BeamOutputMeta) stepMeta.getStepMetaInterface();

      // This is the end of a computing chain, we write out the results
      //
      return input

        // We read a bunch of Strings, one per line basically
        //
        .apply( stepMeta.getName() + " READ FILE", ParDo.of( new KettleToStringFn( fileDefinition, rowMeta ) ) )

        // We need to transform these lines into Kettle fields
        //
        .apply( TextIO.write().to( beamOutputMeta.getFilePrefix() ).withSuffix( ".csv" ) )
        ;


    } catch ( Exception e ) {
      e.printStackTrace();
      numErrors.inc();
      LOG.error( "Error in beam input transform", e );
      return null;
    }

  }
}
