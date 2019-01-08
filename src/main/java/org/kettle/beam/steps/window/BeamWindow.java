package org.kettle.beam.steps.window;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.dummytrans.DummyTrans;

/** Behaves like a Dummy step, simply passing rows in the regular runner.
 *  In Beam this code isn't used.  So the data passing in Dummy only is useful for unit testing and development in Spoon.
 *
 */
public class BeamWindow extends DummyTrans implements StepInterface {

  public BeamWindow( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

}
