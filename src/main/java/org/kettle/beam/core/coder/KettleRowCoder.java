package org.kettle.beam.core.coder;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

public class KettleRowCoder extends AtomicCoder<KettleRow> {

  @Override public void encode( KettleRow value, OutputStream outStream ) throws CoderException, IOException {

    Object[] row = value.getRow();
    ObjectOutputStream out = new ObjectOutputStream( outStream );

    // Length
    //
    if ( row == null ) {
      out.writeInt( -1 );
      return; // all done
    } else {
      out.writeInt( row.length );
    }

    // The values
    //
    for ( int i = 0; i < row.length; i++ ) {
      Object object = row[ i ];
      // Null?
      //
      out.writeBoolean( object == null );

      if ( object != null ) {
        // Type?
        //
        int objectType = getObjectType( object );
        out.writeInt( objectType );

        // The object itself
        //
        write( out, objectType, object );
      }
    }
    out.flush();
  }


  @Override public KettleRow decode( InputStream inStream ) throws CoderException, IOException {

    ObjectInputStream in = new ObjectInputStream( inStream );

    Object[] row = null;
    int length = in.readInt();
    if ( length < 0 ) {
      return new KettleRow( row );
    }
    row = new Object[ length ];
    for ( int i = 0; i < length; i++ ) {
      // Null?
      boolean isNull = in.readBoolean();
      if ( !isNull ) {
        int objectType = in.readInt();
        Object object = read( in, objectType );
        row[i] = object;
      }
    }

    return new KettleRow(row);
  }

  @Override public void verifyDeterministic() throws NonDeterministicException {
    // Sure
  }



  private void write( ObjectOutputStream out, int objectType, Object object ) throws IOException {
    switch ( objectType ) {
      case ValueMetaInterface.TYPE_STRING: {
        String string = (String) object;
        out.writeUTF( string );
      }
      break;
      case ValueMetaInterface.TYPE_INTEGER: {
        Long lng = (Long) object;
        out.writeLong( lng );
      }
      break;
      case ValueMetaInterface.TYPE_DATE: {
        Long lng = ( (Date) object ).getTime();
        out.writeLong( lng );
      }
      break;
      case ValueMetaInterface.TYPE_BOOLEAN: {
        Long lng = ( (Date) object ).getTime();
        out.writeLong( lng );
      }
      break;
      case ValueMetaInterface.TYPE_NUMBER: {
        Double dbl = (Double) object;
        out.writeDouble( dbl );
      }
      break;
      case ValueMetaInterface.TYPE_BIGNUMBER: {
        BigDecimal bd = (BigDecimal) object;
        out.writeUTF( bd.toString() );
      }
      break;
      default:
        throw new IOException( "Data type not supported yet: " + objectType + " - " + object.toString() );
    }
  }

  private Object read( ObjectInputStream in, int objectType ) throws IOException {
    switch ( objectType ) {
      case ValueMetaInterface.TYPE_STRING: {
        String string = in.readUTF();
        return string;
      }

      case ValueMetaInterface.TYPE_INTEGER: {
        Long lng = in.readLong();
        return lng;
      }

      case ValueMetaInterface.TYPE_DATE: {
        Long lng = in.readLong();
        return new Date(lng);
      }

      case ValueMetaInterface.TYPE_BOOLEAN: {
        boolean b = in.readBoolean();
        return b;
      }

      case ValueMetaInterface.TYPE_NUMBER: {
        Double dbl = in.readDouble();
        return dbl;
      }

      case ValueMetaInterface.TYPE_BIGNUMBER: {
        String bd = in.readUTF();
        return new BigDecimal( bd );
      }
      default:
        throw new IOException( "Data type not supported yet: " + objectType );
    }
  }


  private int getObjectType( Object object ) throws CoderException {
    if ( object instanceof String ) {
      return ValueMetaInterface.TYPE_STRING;
    }
    if ( object instanceof Long ) {
      return ValueMetaInterface.TYPE_INTEGER;
    }
    if ( object instanceof Date ) {
      return ValueMetaInterface.TYPE_DATE;
    }
    if ( object instanceof Timestamp ) {
      return ValueMetaInterface.TYPE_TIMESTAMP;
    }
    if ( object instanceof Boolean ) {
      return ValueMetaInterface.TYPE_BOOLEAN;
    }
    if ( object instanceof Double) {
      return ValueMetaInterface.TYPE_NUMBER;
    }
    if ( object instanceof BigDecimal ) {
      return ValueMetaInterface.TYPE_BIGNUMBER;
    }
    throw new CoderException( "Data type for object class "+object.getClass().getName()+" isn't supported yet" );
  }


}
