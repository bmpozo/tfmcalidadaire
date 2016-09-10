import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;

public class javatrafico {

  public static void main(String argv[]) {

    try {

        	File fXmlFile = new File("/home/borja/Documentos/pruebasjava/trafico.xml");
        	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        	Document doc = dBuilder.parse(fXmlFile);

          float totalvehiculostunel = 0;
          float totalvehiculoscalle30 = 0;
          float velocidadmediatunel = 0;
          float velocidadmediasuperficie = 0;

        	doc.getDocumentElement().normalize();

        	System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

        	NodeList nList = doc.getElementsByTagName("DatoGlobal");

        	System.out.println("----------------------------");

          if ( nList.getLength() != 0) {
            for (int temp = 0; temp < 4; temp++) {

              Node nNode = nList.item(temp);

              System.out.println("\nCurrent Element :" + nNode.getNodeName());

              if (nNode.getNodeType() == Node.ELEMENT_NODE) {

                Element eElement = (Element) nNode;
                switch (temp) {
                  case 0: totalvehiculostunel = Float.parseFloat(eElement.getElementsByTagName("VALOR").item(0).getTextContent());
                          System.out.println("totalvehiculostunel : " + totalvehiculostunel);
                          break;
                  case 1: totalvehiculoscalle30 = Float.parseFloat(eElement.getElementsByTagName("VALOR").item(0).getTextContent());
                          System.out.println("totalvehiculoscalle30 : " + totalvehiculoscalle30);
                          break;
                  case 2: velocidadmediatunel = Float.parseFloat(eElement.getElementsByTagName("VALOR").item(0).getTextContent());
                          System.out.println("velocidadmediatunel : " + velocidadmediatunel);
                          break;
                  case 3: velocidadmediasuperficie = Float.parseFloat(eElement.getElementsByTagName("VALOR").item(0).getTextContent());
                          System.out.println("velocidadmediasuperficie : " + velocidadmediasuperficie);
                          break;
                }
                
              }
             }
          }
          else {
            System.out.println("Arbol xml vacio");
          }
    } catch (Exception e) {
	       e.printStackTrace();
    }
  }
}
