package org.kettle.beam.steps.beaminput;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.TransMetaTransform;
import org.kettle.beam.metastore.FileDefinition;

public class BeamInputTransform extends TransMetaTransform {

  private FileDefinition fileDefinition;

  public BeamInputTransform( FileDefinition fileDefinition ) {
    this.fileDefinition = fileDefinition;
  }

  /**
   * Gets fileDefinition
   *
   * @return value of fileDefinition
   */
  public FileDefinition getFileDefinition() {
    return fileDefinition;
  }

  /**
   * @param fileDefinition The fileDefinition to set
   */
  public void setFileDefinition( FileDefinition fileDefinition ) {
    this.fileDefinition = fileDefinition;
  }
}
