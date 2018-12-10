package org.kettle.beam.perspective;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class BeamHelperTest {

  // This is just for tinkering and debugging
  //

  @Ignore
  public void testLibraries() throws Exception {


    List<String> libs = BeamHelper.findLibraryFilesToStage("/home/matt/pentaho/latest", "Neo4JOutput", true);

    assertTrue( libs.size()>0 );

  }
}
