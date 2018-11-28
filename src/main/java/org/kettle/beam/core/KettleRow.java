package org.kettle.beam.core;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder( AvroCoder.class)
public class KettleRow {

  private Object[] row;

  public KettleRow() {
  }

  public KettleRow( Object[] row ) {
    this.row = row;
  }

  /**
   * Gets row
   *
   * @return value of row
   */
  public Object[] getRow() {
    return row;
  }

  /**
   * @param row The row to set
   */
  public void setRow( Object[] row ) {
    this.row = row;
  }
}
