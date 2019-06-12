package core.metastore;

import org.junit.Test;
import org.kettle.beam.core.metastore.SerializableMetaStore;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreAttribute;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class SerializableMetaStoreTest {

  public static final String NAMESPACE = "kettle";
  public static final String TYPE_A = "Type A";
  public static final String DESCRIPTION_OF_TYPE_A = "Description of Type A";
  public static final String ELEMENT_A_1 = "Element A1";
  public static final String DESCRIPTION_OF_ELEMENT_A_1 = "Description of Element A1";
  public static final long SOME_LONG = 1234L;

  @Test
  public void testToJson() throws Exception {

    SerializableMetaStore store1 = new SerializableMetaStore();
    store1.setName( "Serialization test metastore" );
    store1.createNamespace( NAMESPACE );

    IMetaStoreElementType typeA = store1.newElementType( NAMESPACE );
    typeA.setName( TYPE_A );
    typeA.setDescription( DESCRIPTION_OF_TYPE_A );
    store1.createElementType( NAMESPACE, typeA );

    IMetaStoreElement elementA1 = store1.newElement( typeA, ELEMENT_A_1, null );
    elementA1.addChild( store1.newAttribute( "name", ELEMENT_A_1 ) );
    elementA1.addChild( store1.newAttribute( "someInt", SOME_LONG ) );
    store1.createElement( NAMESPACE, typeA, elementA1 );

    String json = store1.toJson();

    SerializableMetaStore store2 = new SerializableMetaStore( json );
    assertEqualMetastores(store1, store2);
  }

  private void assertEqualMetastores( IMetaStore store1, IMetaStore store2 ) throws MetaStoreException {
    List<String> namespaces1 = store1.getNamespaces();
    List<String> namespaces2 = store2.getNamespaces();
    assertEquals( namespaces1.size(), namespaces2.size());

    for (int n=0;n<namespaces1.size();n++) {
      String namespace1 = namespaces1.get( n );
      String namespace2 = namespaces2.get( n );
      assertEquals( namespace1, namespace2 );

      List<IMetaStoreElementType> elementTypes1 = store1.getElementTypes( namespace1 );
      List<IMetaStoreElementType> elementTypes2 = store2.getElementTypes( namespace2 );
      assertEquals( elementTypes1.size(), elementTypes2.size() );
      for (int t=0;t<elementTypes1.size();t++) {
        IMetaStoreElementType elementType1 = elementTypes1.get(t);
        IMetaStoreElementType elementType2 = elementTypes2.get(t);
        assertEquals(elementType1.getName(), elementType2.getName());
        assertEquals(elementType1.getDescription(), elementType2.getDescription());

        List<IMetaStoreElement> elements1 = store1.getElements( namespace1, elementType1 );
        List<IMetaStoreElement> elements2 = store2.getElements( namespace2, elementType2 );
        assertEquals( elements1.size(), elements2.size() );
        for (int e=0;e<elements1.size();e++) {

          IMetaStoreElement element1 = elements1.get(e);
          IMetaStoreElement element2 = elements2.get(e);

          assertEquals( element1.getName(), element2.getName() );

          assertEqualAttributes(element1, element2);
        }
      }
    }
  }

  private void assertEqualAttributes( IMetaStoreAttribute element1, IMetaStoreAttribute element2 ) {
    assertEquals(element1.getId(), element2.getId());
    assertEquals(element1.getValue(), element2.getValue());

    List<IMetaStoreAttribute> children1 = element1.getChildren();
    List<IMetaStoreAttribute> children2 = element2.getChildren();
    for (int c=0;c<children1.size();c++) {
      IMetaStoreAttribute child1 = children1.get(c);
      IMetaStoreAttribute child2 = children2.get(c);
      assertEqualAttributes( child1, child2 );
    }
  }

}
