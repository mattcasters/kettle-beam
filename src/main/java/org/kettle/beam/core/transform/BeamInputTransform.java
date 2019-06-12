package org.kettle.beam.core.transform;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.StringToKettleFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

public class BeamInputTransform extends PTransform<PBegin, PCollection<KettleRow>> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepname;
  private String inputLocation;
  private String separator;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamInputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamInputError" );

  public BeamInputTransform() {
  }

  public BeamInputTransform( @Nullable String name, String stepname, String inputLocation, String separator, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( name );
    this.stepname = stepname;
    this.inputLocation = inputLocation;
    this.separator = separator;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PCollection<KettleRow> expand( PBegin input ) {

    try {
      // Only initialize once on this node/vm
      //
      BeamKettle.init(stepPluginClasses, xpPluginClasses);

      // System.out.println("-------------- TextIO.Read from "+inputLocation+" (UNCOMPRESSED)");

      TextIO.Read ioRead = TextIO.read()
        .from( inputLocation )
        .withCompression( Compression.UNCOMPRESSED )
        ;

      StringToKettleFn stringToKettleFn = new StringToKettleFn( stepname, rowMetaJson, separator, stepPluginClasses, xpPluginClasses );

      PCollection<KettleRow> output = input

        // We read a bunch of Strings, one per line basically
        //
        .apply( stepname + " READ FILE",  ioRead )

        // We need to transform these lines into Kettle fields
        //
        .apply( stepname, ParDo.of( stringToKettleFn ) );

      return output;

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in beam input transform", e );
      throw new RuntimeException( "Error in beam input transform", e );
    }

  }


}
