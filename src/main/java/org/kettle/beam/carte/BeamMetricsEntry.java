package org.kettle.beam.carte;

import java.util.Date;

import org.pentaho.di.www.SlaveServerTransStatus;

public class BeamMetricsEntry {
  /** the carte Object ID of the transformation, some unique number */
  private String carteObjectId;

  /** the name of the transformation */
  private String trans;

  /** The internal Job ID (Spark, Flink, ...)*/
  private String internalJobId;

  /** The update date */
  private Date updateDate;

  /**
   * The transformation status.
   */
  private SlaveServerTransStatus transStatus;

  public BeamMetricsEntry() {
  }

  public BeamMetricsEntry( String carteObjectId, String trans, String internalJobId, Date updateDate, SlaveServerTransStatus transStatus ) {
    this.carteObjectId = carteObjectId;
    this.trans = trans;
    this.internalJobId = internalJobId;
    this.updateDate = updateDate;
    this.transStatus = transStatus;
  }

  /**
   * Gets carteObjectId
   *
   * @return value of carteObjectId
   */
  public String getCarteObjectId() {
    return carteObjectId;
  }

  /**
   * @param carteObjectId The carteObjectId to set
   */
  public void setCarteObjectId( String carteObjectId ) {
    this.carteObjectId = carteObjectId;
  }

  /**
   * Gets trans
   *
   * @return value of trans
   */
  public String getTrans() {
    return trans;
  }

  /**
   * @param trans The trans to set
   */
  public void setTrans( String trans ) {
    this.trans = trans;
  }

  /**
   * Gets internalJobId
   *
   * @return value of internalJobId
   */
  public String getInternalJobId() {
    return internalJobId;
  }

  /**
   * @param internalJobId The internalJobId to set
   */
  public void setInternalJobId( String internalJobId ) {
    this.internalJobId = internalJobId;
  }

  /**
   * Gets transStatus
   *
   * @return value of transStatus
   */
  public SlaveServerTransStatus getTransStatus() {
    return transStatus;
  }

  /**
   * @param transStatus The transStatus to set
   */
  public void setTransStatus( SlaveServerTransStatus transStatus ) {
    this.transStatus = transStatus;
  }

  /**
   * Gets updateDate
   *
   * @return value of updateDate
   */
  public Date getUpdateDate() {
    return updateDate;
  }

  /**
   * @param updateDate The updateDate to set
   */
  public void setUpdateDate( Date updateDate ) {
    this.updateDate = updateDate;
  }
}
