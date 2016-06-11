import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.*;

public class javatiempo {

  public static void main(String argv[]) {

    try {
            // Read the xml document
          	File fXmlFile = new File("/home/borja/Documentos/pruebasjava/tiempo.xml");
            // Get Document to builder
          	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
          	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            // Build document
          	Document doc = dBuilder.parse(fXmlFile);
            // Normalize the XML format
          	doc.getDocumentElement().normalize();
            // Get the root node, in this case the name is the same "root"
            Element root = doc.getDocumentElement();
          	System.out.println("Root element :" + root.getNodeName());
            // Get the node we want -> prediccion. It's a son of root, and it has 7 sons that are our 7 days
            //    Element prediccion = root.getDocumentElement("prediccion");
            NodeList auxpred = root.getElementsByTagName("prediccion");
            System.out.println("numero de elementos al selecionar prediccion :" + auxpred.getLength());
            Node prediccion = auxpred.item(0);
            //    System.out.println("elemento actual :" + prediccion.getNodeName());
            // Get all days
            NodeList nList = prediccion.getChildNodes();
            // To see if there are 7 nodes
            System.out.println("numero de hijos :" + nList.getLength());
            // To see the informacion inside os nList
            for (int temp = 0; temp < nList.getLength(); temp++) {
              Node nNode = nList.item(temp);
              if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                switch (temp) {
                  case 0:
                  case 1: NodeList nListprecipitacion = eElement.getElementsByTagName("prediccion");
                          NodeList nListcielo = eElement.getElementsByTagName("estado_cielo");
                          NodeList nListviento = eElement.getElementsByTagName("viento");
                          NodeList nListtemperatura = eElement.getElementsByTagName("temperatura");
                          // Get all prob_precipitacion nodes
                          Node precipitacion_periodo13 = nListprecipitacion.item(3);
                          Node precipitacion_periodo23 = nListprecipitacion.item(4);
                          Node precipitacion_periodo33 = nListprecipitacion.item(5);
                          // get all estado_cielo nodes
                          Node cielo_periodo13 = nListcielo.item(3);
                          Node cielo_periodo23 = nListcielo.item(4);
                          Node cielo_periodo33 = nListcielo.item(5);
                          // Get all viento nodes
                          Node viento_periodo13 = nListviento.item(3);
                          Node viento_periodo23 = nListviento.item(4);
                          Node viento_periodo33 = nListviento.item(5);
                          // get all values of precipitaciones
                          Float precipitacion1 = Float.parseFloat(precipitacion_periodo13.getTextContent());
                          Float precipitacion2 = Float.parseFloat(precipitacion_periodo23.getTextContent());
                          Float precipitacion3 = Float.parseFloat(precipitacion_periodo33.getTextContent());
                          // get all values of estado_cielo
                          Float cielo1 = Float.parseFloat(cielo_periodo13.getTextContent());
                          Float cielo2 = Float.parseFloat(cielo_periodo23.getTextContent());
                          Float cielo3 = Float.parseFloat(cielo_periodo33.getTextContent());
                          // get all values of viento
                          Float viento1 = Float.parseFloat(viento_periodo13.getTextContent());
                          Float viento2 = Float.parseFloat(viento_periodo23.getTextContent());
                          Float viento3 = Float.parseFloat(viento_periodo33.getTextContent());
                          // Get mean of prob_precipitacion
                          Float precipitacion = (precipitacion1 + precipitacion2 + precipitacion3) / 3;
                          // get mean of viento
                          Float viento = (viento1 + viento2 + viento3) / 3;
                          // get values of temperatura
                          Float tempmax = Float.parseFloat(nListtemperatura.item(0).getTextContent());
                          Float tempmin = Float.parseFloat(nListtemperatura.item(1).getTextContent());
                          Float temperatura1 = Float.parseFloat(nListtemperatura.item(2).getTextContent());
                          Float temperatura2 = Float.parseFloat(nListtemperatura.item(3).getTextContent());
                          Float temperatura3 = Float.parseFloat(nListtemperatura.item(4).getTextContent());
                          Float temperatura4 = Float.parseFloat(nListtemperatura.item(5).getTextContent());
                          Float temperatura = (temperatura1+temperatura2+temperatura3+temperatura4)/4;
                          break;
                  case 2:
                  case 3: NodeList nListprecipitacion = eElement.getElementsByTagName("prediccion");
                          NodeList nListcielo = eElement.getElementsByTagName("estado_cielo");
                          NodeList nListviento = eElement.getElementsByTagName("viento");
                          NodeList nListtemperatura = eElement.getElementsByTagName("temperatura");
                          // Get all prob_precipitacion nodes
                          Node precipitacion_periodo12 = nListprecipitacion.item(3);
                          Node precipitacion_periodo22 = nListprecipitacion.item(4);
                          // get all estado_cielo nodes
                          Node cielo_periodo12 = nListcielo.item(3);
                          Node cielo_periodo22 = nListcielo.item(4);
                          // Get all viento nodes
                          Node viento_periodo12 = nListviento.item(3);
                          Node viento_periodo22 = nListviento.item(4);
                          // get all values of precipitaciones
                          Float precipitacion1 = Float.parseFloat(precipitacion_periodo12.getTextContent());
                          Float precipitacion2 = Float.parseFloat(precipitacion_periodo22.getTextContent());
                          // get all values of estado_cielo
                          Float cielo1 = Float.parseFloat(cielo_periodo12.getTextContent());
                          Float cielo2 = Float.parseFloat(cielo_periodo22.getTextContent());
                          // get all values of viento
                          Float viento1 = Float.parseFloat(viento_periodo12.getTextContent());
                          Float viento2 = Float.parseFloat(viento_periodo22.getTextContent());
                          // Get mean of prob_precipitacion
                          Float precipitacion = (precipitacion1 + precipitacion2) / 2;
                          // get mean of viento
                          Float viento = (viento1 + viento2) / 2;
                          // get values of temperatura
                          Float tempmax = Float.parseFloat(nListtemperatura.item(0).getTextContent());
                          Float tempmin = Float.parseFloat(nListtemperatura.item(1).getTextContent());
                          break;
                  case 4:
                  case 5:
                  case 6: NodeList nListprecipitacion = eElement.getElementsByTagName("prediccion");
                          NodeList nListcielo = eElement.getElementsByTagName("estado_cielo");
                          NodeList nListviento = eElement.getElementsByTagName("viento");
                          NodeList nListtemperatura = eElement.getElementsByTagName("temperatura");
                          // get all values of precipitaciones
                          Float precipitacion = Float.parseFloat(nListprecipitacion.item(0).getTextContent());
                          // get all values of estado_cielo
                          Float cielo1 = Float.parseFloat(nListviento.item(0).getTextContent());
                          // get all values of viento
                          Float viento1 = Float.parseFloat(viento_periodo12.item(0).getTextContent());
                          // get values of temperatura
                          Float tempmax = Float.parseFloat(nListtemperatura.item(0).getTextContent());
                          Float tempmin = Float.parseFloat(nListtemperatura.item(1).getTextContent());
                          break;
                }
                System.out.println("Node Name = " + node.getNodeName());
              }
            }
    }
    catch (Exception e) {
	          e.printStackTrace();
    }
  }
  /*private static void visitChildNodes(NodeList nList) {
           for (int temp = 0; temp < nList.getLength(); temp++)
           {
              Node node = nList.item(temp);
              if (node.getNodeType() == Node.ELEMENT_NODE)
              {
                 System.out.println("Node Name = " + node.getNodeName() + "; Value = " + node.getTextContent());
                 //Check all attributes
                 if (node.hasAttributes()) {
                    // get attributes names and values
                    NamedNodeMap nodeMap = node.getAttributes();
                    for (int i = 0; i < nodeMap.getLength(); i++)
                    {
                        Node tempNode = nodeMap.item(i);
                        System.out.println("Attr name : " + tempNode.getNodeName()+ "; Value = " + tempNode.getNodeValue());
                    }
                    if (node.hasChildNodes()) {
                       //We got more childs; Let's visit them as well
                       visitChildNodes(node.getChildNodes());
                    }
                }
              }
           }
    }*/
}
