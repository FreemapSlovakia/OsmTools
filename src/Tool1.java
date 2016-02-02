import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;


/**
 *
 *
 * @author Martin Ždila
 */
public class Tool1 {

	private static final QName ID = new QName("id");
	private static final QName WAY = new QName("way");
	private static final QName TAG = new QName("tag");
	private static final QName NODE = new QName("node");
	private static final QName ND = new QName("nd");
	private static final QName REF = new QName("ref");
	private static final QName K = new QName("k");
	private static final QName RELATION = new QName("relation");

	private static class DynaIntArray {
		int length;
		int[] array = new int[1000];

		void add(final int n) {
			if (array.length == length) {
				final int[] array2 = new int[length + length / 2];
				System.arraycopy(array, 0, array2, 0, length);
				array = array2;
			}
			array[length++] = n;
		}

		void finish() {
			final int[] array2 = new int[length];
			System.arraycopy(array, 0, array2, 0, length);
			Arrays.sort(array2);
			array = array2;
		}
	}


	public static void main(final String[] args) throws IOException, XMLStreamException, FactoryConfigurationError {
		final XMLInputFactory xif = XMLInputFactory.newInstance();
		XMLEventReader xer = xif.createXMLEventReader(new BufferedInputStream(new FileInputStream("/home/martin/slovakia.osm")));

		final DynaIntArray wayIds = new DynaIntArray();
		final DynaIntArray nodeIds = new DynaIntArray();
		Integer wayId = null;

		final int[] nodeIds0 = new int[2001];
		int ptr = 0;

		boolean mark = false;

		System.out.println("Pass 1...");

		while (xer.hasNext()) {
			final XMLEvent event = xer.nextEvent();

			if (event.isStartElement()) {
				final StartElement startElement = event.asStartElement();
				final QName tagName = startElement.getName();
				if (wayId != null) {
					if (tagName.equals(TAG)) {
						mark |= startElement.getAttributeByName(K).getValue().startsWith("marked_trail_edu");
					} else if (tagName.equals(ND)) {
						nodeIds0[ptr++] = Integer.parseInt(startElement.getAttributeByName(REF).getValue());
					}
				} else if (tagName.equals(WAY)) {
					wayId = Integer.valueOf(startElement.getAttributeByName(ID).getValue());
				}
			} else if (event.isEndElement() && event.asEndElement().getName().equals(WAY)) {
				if (mark) {
					wayIds.add(wayId.intValue());
					for (int i = 0; i < ptr; i++) {
						nodeIds.add(nodeIds0[i]);
					}
					mark = false;
				}
				ptr = 0;
				wayId = null;
			}
		}

		xer.close();

		wayIds.finish();
		nodeIds.finish();

		System.out.println("Found " + wayIds.length + " ways having together " + nodeIds.length + " nodes.");
		System.out.println("Pass 2...");

		xer = xif.createXMLEventReader(new BufferedInputStream(new FileInputStream("/home/martin/slovakia.osm")));
		final XMLOutputFactory xof = XMLOutputFactory.newInstance();
		final FileWriter fw = new FileWriter("/home/martin/slovakia-red.osm");
		final XMLEventWriter xew = xof.createXMLEventWriter(fw);

		boolean writeMe = true;

		while (xer.hasNext()) {
			final XMLEvent event = xer.nextEvent();

			if (event.isStartElement()) {
				final StartElement startElement = event.asStartElement();
				final QName tagName = startElement.getName();

				if (tagName.equals(NODE)) {
					writeMe = Arrays.binarySearch(nodeIds.array, Integer.parseInt(startElement.getAttributeByName(ID).getValue())) >= 0;
				} else if (tagName.equals(WAY)) {
					writeMe = Arrays.binarySearch(wayIds.array, Integer.parseInt(startElement.getAttributeByName(ID).getValue())) >= 0;
				} else if (tagName.equals(RELATION)) {
					writeMe = false;
				}

				if (writeMe) {
					xew.add(startElement);
				}
			} else if (event.isEndElement()) {
				final EndElement endElement = event.asEndElement();

				if (writeMe) {
					xew.add(endElement);
				}

				final QName tagName = endElement.getName();
				if (tagName.equals(WAY) || tagName.equals(NODE) || tagName.equals(RELATION)) {
					writeMe = true;
				}
			} else if (writeMe) {
				xew.add(event);
			}
		}

		xer.close();
		xew.close();
		fw.close();

		System.out.println("Done.");
	}

}
