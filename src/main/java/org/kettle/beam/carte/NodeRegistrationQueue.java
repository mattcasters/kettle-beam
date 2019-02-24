package org.kettle.beam.carte;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.pentaho.di.core.logging.LogChannel;

public class NodeRegistrationQueue {
  private static NodeRegistrationQueue registrationQueue;
  
  private Queue<NodeRegistrationEntry> entryQueue;
  
  public synchronized static NodeRegistrationQueue getInstance() {
    if (registrationQueue==null) {
      registrationQueue = new NodeRegistrationQueue();
    }
    return registrationQueue;
  }
  
  private NodeRegistrationQueue() {
    entryQueue = new ConcurrentLinkedQueue<NodeRegistrationEntry>();
  }
  
  public void addNodeRegistryEntry(NodeRegistrationEntry entry) {
    entryQueue.add(entry);

    LogChannel.GENERAL.logDetailed((entry.isUpdate()?"Update":"Registration")+
        " received for job id : "+entry.getJobId()+", trans="+entry.getTrans()+ ", hostname: "+
        entry.getHostname()+", port: "+entry.getPort());
  }

  public NodeRegistrationEntry pollEntry() {
    return entryQueue.poll();
  }
}
