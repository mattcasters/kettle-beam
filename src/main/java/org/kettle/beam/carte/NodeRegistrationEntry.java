package org.kettle.beam.carte;

import java.util.Date;

import org.pentaho.di.www.SlaveServerTransStatus;

public class NodeRegistrationEntry {
  /** one of "mapper", "reduder" or "combiner" */
  private EntryType type;
  
  /** the hostname or IP address on which the mapper or reducer runs */
  private String hostname; 
  
  /** the port */
  private String port; 
  
  /** the user to connect to the remote server with */
  private String user; 
  
  /** the password to connect to the remote server with */
  private String pass; 
  
  /** the name of the transformation */
  private String trans; 
  
  /** the carte Object ID of the transformation */
  private String carteObjectId;

  /** the Id of the hadoop job */
  private String jobId;

  /** The registration date */
  private Date registrationDate;

  /** The Hadoop task ID */
  private String taskId;
  
  /** Is this an update? */
  private boolean update;

  /**
   * The transformation status.
   */
  private SlaveServerTransStatus transStatus;
  

  public NodeRegistrationEntry(String jobId, String taskId, String hostname, String port, EntryType type, String user, String pass,
      String trans, String carteObjectId, SlaveServerTransStatus transStatus, boolean update) {
    this.jobId = jobId;
    this.taskId = taskId;
    this.hostname = hostname;
    this.port = port;
    this.type = type;
    this.user = user;
    this.pass = pass;
    this.trans = trans;
    this.carteObjectId = carteObjectId;
    this.transStatus = transStatus;
    this.update = update;
    
    this.registrationDate = new Date();
    
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (!(obj instanceof NodeRegistrationEntry)) return false;
    
    NodeRegistrationEntry entry = (NodeRegistrationEntry)obj;
    
    return jobId.equals(entry.jobId) && taskId.equals(entry.taskId) && hostname.equals(entry.hostname) && port.equals(entry.port) && type==entry.type;
  }
  
  @Override
  public int hashCode() {
    return jobId.hashCode() ^ hostname.hashCode() ^ port.hashCode() ^ type.hashCode();
  }

  public EntryType getType() {
    return type;
  }

  public void setType(EntryType type) {
    this.type = type;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPass() {
    return pass;
  }

  public void setPass(String pass) {
    this.pass = pass;
  }

  public String getTrans() {
    return trans;
  }

  public void setTrans(String trans) {
    this.trans = trans;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public Date getRegistrationDate() {
    return registrationDate;
  }

  public void setRegistrationDate(Date registrationDate) {
    this.registrationDate = registrationDate;
  }

  public String getCarteObjectId() {
    return carteObjectId;
  }

  public void setCarteObjectId(String carteObjectId) {
    this.carteObjectId = carteObjectId;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public SlaveServerTransStatus getTransStatus() {
    return transStatus;
  }

  public void setTransStatus(SlaveServerTransStatus transStatus) {
    this.transStatus = transStatus;
  }

  public boolean isUpdate() {
    return update;
  }

  public void setUpdate(boolean update) {
    this.update = update;
  }
  
  
}
