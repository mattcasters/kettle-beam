package org.kettle.beam.xp;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;

public class BeamTrans extends Trans {

  public BeamTrans( TransMeta transMeta ) {
    super(transMeta);
  }

  public void setFinished( boolean finished) {
    super.setFinished( finished );
  }

  // Always only 1 step copy: 0
  //
  public StepDataInterface getStepDataInterface( String stepname, int stepcopy) {
    return super.getStepDataInterface( stepname, 0 );
  }
}
