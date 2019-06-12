package org.kettle.beam.core.transform;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.KettleToStringFn;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class BeamOutputTransform extends PTransform<PCollection<KettleRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepname;
  private String outputLocation;
  private String filePrefix;
  private String fileSuffix;
  private String separator;
  private String enclosure;
  private String rowMetaJson;
  private boolean windowed;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamOutputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamOutputError" );

  public BeamOutputTransform() {
  }

  public BeamOutputTransform( String stepname, String outputLocation, String filePrefix, String fileSuffix, String separator, String enclosure, boolean windowed, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.stepname = stepname;
    this.outputLocation = outputLocation;
    this.filePrefix = filePrefix;
    this.fileSuffix = fileSuffix;
    this.separator = separator;
    this.enclosure = enclosure;
    this.windowed = windowed;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PDone expand( PCollection<KettleRow> input ) {

    try {
      // Only initialize once on this node/vm
      //
      BeamKettle.init(stepPluginClasses, xpPluginClasses);

      // Inflate the metadata on the node where this is running...
      //
      RowMetaInterface rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      // This is the end of a computing chain, we write out the results
      // We write a bunch of Strings, one per line basically
      //
      PCollection<String> stringCollection = input.apply( stepname, ParDo.of( new KettleToStringFn( stepname, outputLocation, separator, enclosure, rowMetaJson, stepPluginClasses, xpPluginClasses ) ) );

      // We need to transform these lines into a file and then we're PDone
      //
      TextIO.Write write = TextIO.write();
      if ( StringUtils.isNotEmpty(outputLocation)) {
        String outputPrefix = outputLocation;
        if (!outputPrefix.endsWith( File.separator)) {
          outputPrefix+=File.separator;
        }
        if (StringUtils.isNotEmpty( filePrefix )) {
          outputPrefix+=filePrefix;
        }
        write = write.to( outputPrefix );
      }
      if (StringUtils.isNotEmpty( fileSuffix )) {
        write = write.withSuffix( fileSuffix );
      }

      // For streaming data sources...
      //
      if (windowed) {
        write = write.withWindowedWrites().withNumShards( 4 ); // TODO config
      }

      stringCollection.apply(write);

      // Get it over with
      //
      return PDone.in(input.getPipeline());

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in beam output transform", e );
      throw new RuntimeException( "Error in beam output transform", e );
    }
  }


  /**
   * Gets stepname
   *
   * @return value of stepname
   */
  public String getStepname() {
    return stepname;
  }

  /**
   * @param stepname The stepname to set
   */
  public void setStepname( String stepname ) {
    this.stepname = stepname;
  }

  /**
   * Gets outputLocation
   *
   * @return value of outputLocation
   */
  public String getOutputLocation() {
    return outputLocation;
  }

  /**
   * @param outputLocation The outputLocation to set
   */
  public void setOutputLocation( String outputLocation ) {
    this.outputLocation = outputLocation;
  }

  /**
   * Gets filePrefix
   *
   * @return value of filePrefix
   */
  public String getFilePrefix() {
    return filePrefix;
  }

  /**
   * @param filePrefix The filePrefix to set
   */
  public void setFilePrefix( String filePrefix ) {
    this.filePrefix = filePrefix;
  }

  /**
   * Gets fileSuffix
   *
   * @return value of fileSuffix
   */
  public String getFileSuffix() {
    return fileSuffix;
  }

  /**
   * @param fileSuffix The fileSuffix to set
   */
  public void setFileSuffix( String fileSuffix ) {
    this.fileSuffix = fileSuffix;
  }

  /**
   * Gets separator
   *
   * @return value of separator
   */
  public String getSeparator() {
    return separator;
  }

  /**
   * @param separator The separator to set
   */
  public void setSeparator( String separator ) {
    this.separator = separator;
  }

  /**
   * Gets enclosure
   *
   * @return value of enclosure
   */
  public String getEnclosure() {
    return enclosure;
  }

  /**
   * @param enclosure The enclosure to set
   */
  public void setEnclosure( String enclosure ) {
    this.enclosure = enclosure;
  }

  /**
   * Gets inputRowMetaJson
   *
   * @return value of inputRowMetaJson
   */
  public String getRowMetaJson() {
    return rowMetaJson;
  }

  /**
   * @param rowMetaJson The inputRowMetaJson to set
   */
  public void setRowMetaJson( String rowMetaJson ) {
    this.rowMetaJson = rowMetaJson;
  }
}
