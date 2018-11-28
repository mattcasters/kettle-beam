package org.kettle.beam.core;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.RowMetaInterface;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

// TODO: Figure out how to set this on the various code parts
// We need to figure out which Row metadata we can use for the KettleRow given
// Normally this works by using different classes in the different processing stages
// We want to be a bit more generic.
//
public class KettleRowCoder extends Coder<KettleRow> {

  private RowMetaInterface rowMeta;

  public KettleRowCoder() {
    this.rowMeta = rowMeta;
  }

  @Override public void encode( KettleRow value, OutputStream outStream ) throws CoderException, IOException {
    try {
      rowMeta.writeData( new DataOutputStream( outStream ), value.getRow() );
    } catch(Exception e) {
      throw new CoderException( "Unable to serialize row : "+rowMeta.toString(), e );
    }
  }

  @Override public KettleRow decode( InputStream inStream ) throws CoderException, IOException {
    try {
      return new KettleRow( rowMeta.readData( new DataInputStream( inStream ) ) );
    } catch ( KettleFileException e ) {
      throw new CoderException( "Unable to serialize row : "+rowMeta.toString(), e );
    }
  }

  @Override public List<? extends Coder<?>> getCoderArguments() {
    return new ArrayList<>(  );
  }

  @Override public void verifyDeterministic() throws NonDeterministicException {

  }
}
