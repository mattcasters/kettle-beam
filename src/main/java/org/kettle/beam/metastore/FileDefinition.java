package org.kettle.beam.metastore;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

import java.util.ArrayList;
import java.util.List;

@MetaStoreElementType(
  name = "File Definition",
  description = "Describes a file layout"
)
public class FileDefinition {

  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private List<FieldDefinition> fieldDefinitions;

  @MetaStoreAttribute
  private String separator;

  @MetaStoreAttribute
  private String enclosure;

  public FileDefinition() {
    fieldDefinitions = new ArrayList<>();
  }

  public FileDefinition( String name, String description, List<FieldDefinition> fieldDefinitions, String separator, String enclosure ) {
    this.name = name;
    this.description = description;
    this.fieldDefinitions = fieldDefinitions;
    this.separator = separator;
    this.enclosure = enclosure;
  }

  public RowMetaInterface getRowMeta() throws KettlePluginException {
    RowMetaInterface rowMeta = new RowMeta();

    for (FieldDefinition fieldDefinition : fieldDefinitions) {
      rowMeta.addValueMeta( fieldDefinition.getValueMeta() );
    }

    return rowMeta;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets fieldDefinitions
   *
   * @return value of fieldDefinitions
   */
  public List<FieldDefinition> getFieldDefinitions() {
    return fieldDefinitions;
  }

  /**
   * @param fieldDefinitions The fieldDefinitions to set
   */
  public void setFieldDefinitions( List<FieldDefinition> fieldDefinitions ) {
    this.fieldDefinitions = fieldDefinitions;
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
}
