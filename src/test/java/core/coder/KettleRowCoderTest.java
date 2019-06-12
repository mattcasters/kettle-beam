package core.coder;

import junit.framework.TestCase;
import org.junit.Test;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.coder.KettleRowCoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;

public class KettleRowCoderTest extends TestCase {

  ByteArrayOutputStream outputStream;
  private KettleRowCoder kettleRowCoder;

  @Override protected void setUp() throws Exception {

    outputStream= new ByteArrayOutputStream( 1000000 );
    kettleRowCoder = new KettleRowCoder();
  }

  @Test
  public void testEncode() throws IOException {

    KettleRow row1 = new KettleRow(new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) } );

    kettleRowCoder.encode( row1, outputStream );
    outputStream.flush();
    outputStream.close();
    byte[] bytes = outputStream.toByteArray();

    ByteArrayInputStream inputStream = new ByteArrayInputStream( bytes );
    KettleRow row1d = kettleRowCoder.decode( inputStream );

    assertEquals( row1, row1d );
  }


  @Test
  public void decode() {
  }
}