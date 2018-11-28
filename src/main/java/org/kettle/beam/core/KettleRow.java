package org.kettle.beam.core;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

public class KettleRow implements Serializable {

  private Object[] row;

  public KettleRow() {
  }

  public KettleRow( Object[] row ) {
    this.row = row;
  }

  /*
  private void writeObject( ObjectOutputStream out ) throws IOException {

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
      default:
        throw new IOException( "Data type not supported yet: " + objectType + " - " + object.toString() );
    }
  }

  private int getObjectType( Object object ) {
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
    if ( object instanceof BigDecimal ) {
      return ValueMetaInterface.TYPE_BIGNUMBER;
    }
    return -1;
  }

  private void readObject( ObjectInputStream in ) throws IOException, ClassNotFoundException {
    row = null;
    int length = in.readInt();
    if ( length < 0 ) {
      return;
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

      default:
        throw new IOException( "Data type not supported yet: " + objectType );
    }
  }

  private void readObjectNoData() throws ObjectStreamException {
    row = new Object[ 0 ];
  }

*/

  /**
   * Gets row
   *
   * @return value of row
   */
  public Object[] getRow() {
    return row;
  }

  /**
   * @param row The row to set
   */
  public void setRow( Object[] row ) {
    this.row = row;
  }
}
