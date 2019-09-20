/*
 * Copyright 2017 Hitachi America, Ltd., R&D.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kettle.beam.perspective;

import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

public class BeamMenuController extends AbstractXulEventHandler implements ISpoonMenuController {

  private static final Class<?> PKG = BeamController.class;

  private BeamController beamController;

  public void setBeamController( BeamController beamController ) {
    this.beamController = beamController;
  }


  @Override public void updateMenu( Document document ) {
    // TODO
  }
}