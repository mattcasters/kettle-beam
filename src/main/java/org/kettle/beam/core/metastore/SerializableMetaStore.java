package org.kettle.beam.core.metastore;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreAttribute;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.util.List;

/**
 * This metastore implementation is an in-memory metastore which serializes using JSON
 */
public class SerializableMetaStore extends MemoryMetaStore implements IMetaStore {
  public SerializableMetaStore() {
    super();
  }

  /**
   * Create a copy of all elements in an existing metastore.
   *
   * @param source the source store to copy over
   */
  public SerializableMetaStore(IMetaStore source) throws MetaStoreException {
    List<String> srcNamespaces = source.getNamespaces();
    for (String srcNamespace : srcNamespaces) {
      createNamespace( srcNamespace );
      List<IMetaStoreElementType> srcElementTypes = source.getElementTypes( srcNamespace );
      for (IMetaStoreElementType srcElementType : srcElementTypes) {

        IMetaStoreElementType elementType = newElementType( srcNamespace );
        elementType.setName( srcElementType.getName() );
        elementType.setDescription( srcElementType.getDescription() );
        createElementType(srcNamespace, elementType);

        List<IMetaStoreElement> srcElements = source.getElements( srcNamespace, elementType );
        for (IMetaStoreElement srcElement : srcElements) {
          IMetaStoreElement element = newElement();
          element.setName( srcElement.getName() );
          element.setValue( srcElement.getValue() );

          copyChildren(srcElement, element);

          createElement(srcNamespace, elementType, element);
        }
      }
    }
  }

  private void copyChildren( IMetaStoreAttribute srcAttribute, IMetaStoreAttribute attribute ) throws MetaStoreException {
    List<IMetaStoreAttribute> srcChildren = srcAttribute.getChildren();
    for (IMetaStoreAttribute srcChild : srcChildren) {
      IMetaStoreAttribute child = newAttribute( srcChild.getId(), srcChild.getValue() );
      copyChildren(srcChild, child);
      attribute.addChild( child );
    }
  }

  public String toJson() throws MetaStoreException {

    JSONObject jStore = new JSONObject();

    // Metastore name and description
    //
    jStore.put("name", getName());
    jStore.put("description", getDescription());

    // The namespaces
    //
    JSONArray jNamespaces = new JSONArray ();
    jStore.put("namespaces", jNamespaces);

    for (String namespace : getNamespaces()) {
      addNamespace(jNamespaces, namespace);
    }

    return jStore.toJSONString();
  }

  public SerializableMetaStore(String storeJson) throws ParseException, MetaStoreException {
    this();
    JSONParser parser = new JSONParser();
    JSONObject jStore = (JSONObject) parser.parse( storeJson );

    setName((String)jStore.get( "name" ));
    setDescription((String)jStore.get( "description" ));

    JSONArray jNamespaces = (JSONArray) jStore.get("namespaces");
    for (int n=0;n<jNamespaces.size();n++) {
      JSONObject jNamespace = (JSONObject) jNamespaces.get(n);
      readNamespace(jNamespace);
    }

  }

  private void addNamespace( JSONArray jNamespaces, String namespace ) throws MetaStoreException {

    JSONObject jNamespace = new JSONObject();
    jNamespaces.add(jNamespace);

    jNamespace.put("name", namespace);

    JSONArray jElementTypes = new JSONArray();
    jNamespace.put("types", jElementTypes);

    List<IMetaStoreElementType> elementTypes = getElementTypes( namespace );
    for (IMetaStoreElementType elementType : elementTypes) {
      addElementType(jElementTypes, namespace, elementType);
    }
  }

  private void readNamespace( JSONObject jNamespace ) throws MetaStoreException {

    String namespace = (String) jNamespace.get( "name" );
    createNamespace( namespace );

    JSONArray jTypes = (JSONArray) jNamespace.get( "types" );
    for (int t=0;t<jTypes.size();t++) {
      JSONObject jType = (JSONObject) jTypes.get(t);
      readElementType(namespace, jType);
    }
  }

  private void addElementType( JSONArray jElementTypes, String namespace, IMetaStoreElementType elementType ) throws MetaStoreException {
    JSONObject jElementType = new JSONObject();
    jElementTypes.add(jElementType);

    jElementType.put("name", elementType.getName());
    jElementType.put("description", elementType.getDescription());

    JSONArray jElements = new JSONArray ();
    jElementType.put("elements", jElements);

    List<IMetaStoreElement> elements = getElements( namespace, elementType );
    for (IMetaStoreElement element : elements) {
      addElement(jElements, namespace, elementType, element);
    }
  }

  private void readElementType( String namespace, JSONObject jType ) throws MetaStoreException {
    String name = (String) jType.get("name");
    String description = (String) jType.get("description");

    IMetaStoreElementType elementType = newElementType( namespace );
    elementType.setName( name );
    elementType.setDescription( description );
    createElementType( namespace, elementType );

    JSONArray jElements = (JSONArray) jType.get( "elements" );
    for (int e=0;e<jElements.size();e++) {
      JSONObject jElement = (JSONObject) jElements.get(e);
      readElement(namespace, elementType, jElement);
    }

  }

  private void addElement( JSONArray jElements, String namespace, IMetaStoreElementType elementType, IMetaStoreElement element ) {
    JSONObject jElement = new JSONObject();
    jElements.add(jElement);

    jElement.put("id", element.getId());
    jElement.put("name", element.getName());
    jElement.put("value", element.getValue());

    JSONArray jChildren = new JSONArray();
    jElement.put("children", jChildren);
    List<IMetaStoreAttribute> children = element.getChildren();
    for (IMetaStoreAttribute child : children) {
      addChild(jChildren, child);
    }
  }

  private void readElement( String namespace, IMetaStoreElementType elementType, JSONObject jElement ) throws MetaStoreException {

    IMetaStoreElement element = newElement();
    element.setName( (String) jElement.get("name") );

    readChild(element, jElement);

    createElement( namespace, elementType, element );
  }

  private void addChild( JSONArray jChildren, IMetaStoreAttribute child ) {
    JSONObject jChild = new JSONObject();
    jChildren.add(jChild);

    jChild.put("id", child.getId());
    jChild.put("value", child.getValue());
    JSONArray jSubChildren = new JSONArray();
    jChild.put("children", jSubChildren);
    List<IMetaStoreAttribute> subChildren = child.getChildren();
    for (IMetaStoreAttribute subChild : subChildren) {
      addChild(jSubChildren, subChild);
    }
  }

  private void readChild( IMetaStoreAttribute parent, JSONObject jChild ) throws MetaStoreException {
    parent.setId((String) jChild.get("id"));
    parent.setValue( jChild.get("value") );

    JSONArray jSubChildren = (JSONArray) jChild.get("children");
    for (int c=0;c<jSubChildren.size();c++) {
      JSONObject jSubChild = (JSONObject) jSubChildren.get( c );
      IMetaStoreAttribute subAttribute = newAttribute(null, null);
      readChild( subAttribute, jSubChild );
      parent.addChild( subAttribute );
    }
  }
}
