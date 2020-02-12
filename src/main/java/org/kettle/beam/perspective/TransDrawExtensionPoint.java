package org.kettle.beam.perspective;

import org.apache.commons.lang.StringUtils;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.gui.PrimitiveGCInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransPainterExtension;

@ExtensionPoint(
  id = "Beam.TransDrawExtensionPoint",
  description = "Draw batch or single threaded for Beam",
  extensionPointId = "TransPainterStep"
)
public class TransDrawExtensionPoint implements ExtensionPointInterface {
  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof TransPainterExtension ) ) {
      return;
    }
    TransPainterExtension ext = (TransPainterExtension) object;
    boolean batch = "true".equalsIgnoreCase( ext.stepMeta.getAttribute( BeamConst.STRING_KETTLE_BEAM, BeamConst.STRING_STEP_FLAG_BATCH ) );
    boolean single = "true".equalsIgnoreCase( ext.stepMeta.getAttribute( BeamConst.STRING_KETTLE_BEAM, BeamConst.STRING_STEP_FLAG_SINGLE_THREADED ) );
    if (!batch && !single) {
      return;
    }
    String str = "";
    if ( batch ) {
      str += "Batch";
    }
    if ( single ) {
      if ( batch ) {
        str += " / ";
      }
      str += "Single";
    }
    if ( StringUtils.isNotEmpty( str ) ) {
      str="Beam "+str;
      Point strSize = ext.gc.textExtent( str );
      ext.gc.setFont( PrimitiveGCInterface.EFont.NOTE );
      ext.gc.drawText( str, ext.x1 + ext.iconsize, ext.y1 - strSize.y );
    }
  }
}
