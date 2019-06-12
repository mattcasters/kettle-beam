package core;

import org.junit.Test;
import org.kettle.beam.core.KettleRow;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KettleRowTest {

  @Test
  public void equalsTest() {
    Object[] row1 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row2 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    KettleRow kettleRow1 = new KettleRow(row1);
    KettleRow kettleRow2 = new KettleRow(row2);
    assertTrue(kettleRow1.equals( kettleRow2 ));

    Object[] row3 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row4 = new Object[] { "AAA", "CCC", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    KettleRow kettleRow3 = new KettleRow(row3);
    KettleRow kettleRow4 = new KettleRow(row4);
    assertFalse(kettleRow3.equals( kettleRow4 ));

    Object[] row5 = new Object[] { "AAA", null, Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row6 = new Object[] { "AAA", null, Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    KettleRow kettleRow5 = new KettleRow(row5);
    KettleRow kettleRow6 = new KettleRow(row6);
    assertTrue(kettleRow5.equals( kettleRow6 ));

    Object[] row7 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row8 = new Object[] { "AAA", null, Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    KettleRow kettleRow7 = new KettleRow(row7);
    KettleRow kettleRow8 = new KettleRow(row8);
    assertFalse(kettleRow7.equals( kettleRow8 ));
  }

  @Test
  public void hashCodeTest() {
    Object[] row1 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    KettleRow kettleRow1 = new KettleRow(row1);
    assertEquals( -1023250643, kettleRow1.hashCode() );
  }
}