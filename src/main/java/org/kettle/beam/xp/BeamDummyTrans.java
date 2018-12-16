package org.kettle.beam.xp;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTrans;

public class BeamDummyTrans extends DummyTrans implements StepInterface {

  protected boolean init;

  public BeamDummyTrans( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public void setInit( boolean init ) {
    this.init = init;
  }

  @Override public boolean isInitialising() {
    return init;
  }
}