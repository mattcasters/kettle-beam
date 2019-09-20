package org.kettle.beam.perspective;

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

import java.util.ResourceBundle;

import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;

@SpoonPlugin( id = "BeamSpoonPlugin", image = "" )
@SpoonPluginCategories( { "spoon" } )
public class BeamSpoonPlugin implements SpoonPluginInterface, SpoonLifecycleListener {

  private static final Class<?> PKG = BeamSpoonPlugin.class;
  private ResourceBundle resourceBundle = new XulSpoonResourceBundle( PKG );

  // private BeamPerspective perspective;

  public BeamSpoonPlugin() throws XulException {
    // this.perspective = new BeamPerspective();
  }

  @Override
  public void onEvent( SpoonLifeCycleEvent evt ) {
    // TODO Auto-generated method stub
  }

  @Override
  public void applyToContainer( String category, XulDomContainer container ) throws XulException {
    container.registerClassLoader( getClass().getClassLoader() );
    if ( category.equals( "spoon" ) ) {
      container.loadOverlay( "beam_spoon_overlays.xul", resourceBundle );
      container.addEventHandler( BeamHelper.getInstance() );
    }
  }

  @Override
  public SpoonLifecycleListener getLifecycleListener() {
    // TODO Auto-generated method stub
    return this;
  }

  @Override public SpoonPerspective getPerspective() {
    return null;
  }

/*
  @Override
  public SpoonPerspective getPerspective() {
    return perspective;
  }
*/

}