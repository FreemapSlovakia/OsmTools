import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.postgis.PGgeometry;
import org.postgresql.Driver;
import org.postgresql.jdbc4.Jdbc4Connection;


/**
 *
 * opt/osmosis/bin/osmosis --read-pbf slovakia-latest.osm.pbf --tf reject-relations --tf reject-ways --tf accept-nodes name='*'  --write-xml file="slovakia-nodes1.osm"
 * @author Martin Ždila
 */
public class Tool2 {

	private static final QName TAG = new QName("tag");
	private static final QName NODE = new QName("node");
	private static final QName K = new QName("k");
	private static final QName V = new QName("v");
	private static final QName LAT = new QName("lat");
	private static final QName LON = new QName("lon");


	private static class Location {
		double lat;
		double lon;
		String name;
		boolean existing;
	}

	private static final Pattern skipPattern = Pattern.compile(
			"\\d|s\\.r\\.o|\\d[Ss]v\\.|priemysel"
			+ "|\\b(žst\\.|ihr\\.|ul\\.|kultúr|hosp\\.|kult\\.|žel\\.|ošíp\\.|[Cc]hránen|pož.|n\\.p\\.|trf\\.|vod\\.|krav\\.|adm\\.|oš[ií]p\\.)"

			+ "|^(.{0,2}|ovčín|jedáleň|kotolňa|garáže|zlieváreň|jasle|skládka|parkovisko|hostinec|stodola|dielne|šopa|senník|píla"
			+ "|[Hh]ornád|[Dd]unaj|Hnilec|Hron|Topľa|Váh|Ipeľ|Morava|Nitra|Tisa|Slaná|Torysa|Dunajec|VÁH)$"

			+ "|\\b(hala|banka|knižnica|[šsŠ]kola|škôlka|požiarna|zbrojnica|materská|sm[úu]tku|ospodársky|podnik|[Uu]lica"
			+ "|rezervácia|plaváreň|[Úú]rad|horáreň|vodáreň|strelnica|amfiteáter|poliklinika|kúpalisko|klzisko|[Vv]odojem|stredisko|MNV|CHKO|zóna|ústav"
			+ "|potok|[Oo]becný|[Kk]ino|[Pp]amätník|[Ss]tanica|[Dd]ružstvo|[Hh]otel|pivovar|nádrž|salaš|žumpa|senník|[pP]olícia|[Ii]hrisko"
			+ "|bazén|dielňa|sklad|štadión|[Jj]ednota|[Zz]ávod|[Kk]ultúrny|pošta|bchata|pamiatka|kanál|kostol|[Pp]otraviny|telocvičňa|škola|kurín|kravín|ošipáreň|garáž|[Hh]ospodársky"
			+ "|SAD|ZŠ|MŠ|OcÚ|Ob[ÚU]|TJ|OUNZ|ZDŠ|CH[ÚA]|SOU|OÚ|PD|JRD|SPP|ZUŠ|SNP|COOP|SPŠ|[Nn]árodn[áé]|park|rezervácia|[Vv]odojem|fara|lesopark|intravilan|okres|d[oô]chodcov"
			+ "|[Vv]dj|sýpka|šatne|maštaľ|[Čč]istiateň|[Oo]bchodný|budova|[Rr]eštaurácia|[Ss]tanica|šatne|[Nn]emocnica|územie|oblasť|ŽSR|sprchy|plocha|priestor|zariadenie|vrátnica|trafo|rampa)\\b");


	public static void main(final String[] args) throws IOException, XMLStreamException, FactoryConfigurationError, SQLException {
		final XMLInputFactory xif = XMLInputFactory.newInstance();

		System.out.println("reading existing locations");

		final List<Location> locationList = new ArrayList<Location>(); // readNamedNodes();

		// readLocationsFromFiles(xif, locationList);
		readLocationsFromDb(locationList);

		System.out.println("merging duplicates");

		final Map<Location, List<Location>> map = new HashMap<Location, List<Location>>();

		int n = 0;
		final Location[] locations = locationList.toArray(new Location[0]);
		for (int i = 0; i < locations.length; i++) {
			final Location l1 = locations[i];
			for (int j = i + 1; j < locations.length - 1; j++) {
				final Location l2 = locations[j];
				final double limitDist = l1.existing || l2.existing ? 4000 : 2000; //  5km for existing locations, 2km for others
				if (l1.name.equalsIgnoreCase(l2.name) && distFrom(l1.lat, l1.lon, l2.lat, l2.lon) < limitDist) {
					put(map, l1, l2);
					put(map, l2, l1);
					if (++n % 100 == 0) {
						System.out.println("dup: " + n);
					}
				}
			}
		}

		final FileWriter fw = new FileWriter("/home/martin/locations.osm");
		final XMLOutputFactory xof = XMLOutputFactory.newInstance();
		final XMLStreamWriter xsw = xof.createXMLStreamWriter(fw);

		xsw.writeStartDocument("UTF-8", "1.0");
		xsw.writeStartElement("osm");
		xsw.writeAttribute("version", "0.6");

		final Set<Location> skipSet = new HashSet<Location>();
		int id = -1;
		for (final Location loc : locations) {
			if (skipSet.contains(loc)) {
//				System.out.println("[skip]");
				continue;
			}

			final Set<Location> localSkipSet = new HashSet<Location>();
			final boolean existing = resolve(map, localSkipSet, loc);
			skipSet.addAll(localSkipSet);

			if (existing) {
				continue;
			}

			final Location result;
			if (localSkipSet.isEmpty()) {
				//System.out.println(loc.name + " " + loc.lat + " " + loc.lon);
				result = loc;
			} else {
				result = average(localSkipSet.toArray(new Location[0]));
//				System.out.println(loc.name + " ??? ???");

			}

			xsw.writeStartElement("node");
			xsw.writeAttribute("id", Integer.toString(id--));
			xsw.writeAttribute("lat", Double.toString(result.lat));
			xsw.writeAttribute("lon", Double.toString(result.lon));

//			xsw.writeEmptyElement("tag");
//			xsw.writeAttribute("k", "place");
//			xsw.writeAttribute("v", "locality");

//			xsw.writeEmptyElement("tag");
//			xsw.writeAttribute("k", "import_ref");
//			xsw.writeAttribute("v", "kapor_names");

			xsw.writeEmptyElement("tag");
			xsw.writeAttribute("k", "source");
			xsw.writeAttribute("v", "kapor2");

			xsw.writeEmptyElement("tag");
			xsw.writeAttribute("k", "name");
			xsw.writeAttribute("v", result.name);

			xsw.writeEndElement();
		}

		xsw.writeEndElement();
		xsw.writeEndDocument();

		xsw.close();
		fw.close();

		System.out.println("Done.");
	}

	private static void readLocationsFromDb(final List<Location> locationList) throws SQLException {
		final Properties properties = new Properties();
		properties.setProperty("user", "martin");
		properties.setProperty("password", "b0n0");
		final Connection connection = new Driver().connect("jdbc:postgresql://localhost:5432/gis", properties);
		try {
			((Jdbc4Connection) connection).addDataType("geometry", "org.postgis.PGgeometry");
			((Jdbc4Connection) connection).addDataType("box3d", "org.postgis.PGbox3d");

			final Statement stmt = connection.createStatement();
			final String sql =
//					"SELECT name, ST_Centroid(way) FROM planet_osm_point WHERE name IS NOT NULL UNION "
//					+ "SELECT name, ST_Centroid(way) FROM planet_osm_line WHERE name IS NOT NULL UNION "
//			+ "SELECT name, ST_Centroid(way) FROM planet_osm_point WHERE name IS NOT NULL UNION ";


			"select st_centroid(kataster.geom2), kataster.nazov from kataster left join exist_nazvy on layer = 'popis_text' and kataster.key = exist_nazvy.key where exist_nazvy.geom2 is null and  layer = 'popis_text'";

			final ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
		    	final org.postgis.Point point = (org.postgis.Point) ((PGgeometry) rs.getObject(1)).getGeometry();

		    	final Location location = new Location();
				location.lat = point.getY();
				location.lon = point.getX();
				location.name = rs.getString(2);

				if (!skipPattern.matcher(location.name).find()) {
					locationList.add(location);
					//System.out.println(rs.getString(2) + " " + point.getX() + " " + point.getY());
				}
			}
			rs.close();
			stmt.close();
		} finally {
			connection.close();
		}
	}

	private static void readLocationsFromFiles(final XMLInputFactory xif, final List<Location> locationList) throws XMLStreamException, FileNotFoundException {
		//		int ii = 0;

		Location location = null;

		for (final File file : new File("/home/martin/names").listFiles()) {
//			if (ii++ > 10) {
//				break;
//			}

			System.out.println("reading " + file);
			final XMLEventReader xer = xif.createXMLEventReader(new BufferedInputStream(new FileInputStream(file)));
			while (xer.hasNext()) {
				final XMLEvent event = xer.nextEvent();


				if (event.isStartElement()) {
					final StartElement startElement = event.asStartElement();
					final QName tagName = startElement.getName();
					if (tagName.equals(NODE)) {
						location = new Location();
						location.lat = Double.parseDouble(startElement.getAttributeByName(LAT).getValue());
						location.lon = Double.parseDouble(startElement.getAttributeByName(LON).getValue());
					} else if (tagName.equals(TAG) && startElement.getAttributeByName(K).getValue().equals("name") && location != null) {
						location.name = startElement.getAttributeByName(V).getValue();

						if (location.name.matches("(?:\\w )+\\w \\w")) {
							final StringBuilder sb = new StringBuilder();
							for (int i = 0; i <= location.name.length(); i += 2) {
								sb.append(location.name.charAt(i));
							}
							location.name = sb.toString();
							System.out.println(location.name);
						}
					}
				} else if (event.isEndElement()) {
					if (event.asEndElement().getName().equals(NODE) && location != null) {
						if (skipPattern.matcher(location.name).find()) {
//							 System.out.println("Skipping: " + location.name);
						} else {
							locationList.add(location);
						}
						location = null;
					}
				}
			}

			xer.close();
		}
	}

	private static boolean resolve(final Map<Location, List<Location>> map, final Set<Location> skipSet, final Location loc) {
		boolean existing = loc.existing;

		final List<Location> locations = map.remove(loc);
		if (locations == null) {
			return existing;
		}

		skipSet.addAll(locations);
		for (final Location location : locations) {
			existing |= resolve(map, skipSet, location);
		}

		return existing;
	}

	private static void put(final Map<Location, List<Location>> map, final Location l1, final Location l2) {
		List<Location> list = map.get(l1);
		if (list == null) {
			list = new ArrayList<Location>();
			map.put(l1, list);
		}
		list.add(l2);
	}

	private static final double meterConversion = 1609.0;
	private static final double earthRadius = 3958.75;

	public static double distFrom(final double lat1, final double lng1, final double lat2, final double lng2) {
		final double dLat = Math.toRadians(lat2 - lat1);
		final double dLng = Math.toRadians(lng2 - lng1);
		final double a = Math.sin(dLat / 2.0) * Math.sin(dLat / 2.0) +
				Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
				Math.sin(dLng / 2.0) * Math.sin(dLng / 2.0);
		final double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));
		return earthRadius * meterConversion * c;
	}


	// OK for small distances
	private static Location average(final Location[] locations) {
		final int size = locations.length;
        final double[] weights = new double[size];

        for (int i = 0; i < size; i++) {
            final Location c1 = locations[i];
            for (int j = i + 1; j < size; j++) {
                final Location c2 = locations[j];
                final double d = distFrom(c1.lat, c1.lon, c2.lat, c2.lon);
                weights[i] += d;
                weights[j] += d;
            }
        }

        double lat = 0, lon = 0, weight = 0;
        for (int i = 0; i < size; i++) {
            final Location location = locations[i];
            final double w = weights[i];
            lat += location.lat * w;
            lon += location.lon * w;
            weight += w;
        }

        final Location result = new Location();
        result.name = locations[0].name;

        if (weight == 0) {
        	result.lat = locations[0].lat;
        	result.lon = locations[0].lon;
        } else {
        	result.lat = lat / weight;
        	result.lon = lon / weight;
        }

        return result;

	}

	private static List<Location> readNamedNodes() throws FileNotFoundException, XMLStreamException {
		final XMLInputFactory xif = XMLInputFactory.newInstance();
		final XMLEventReader xer = xif.createXMLEventReader(new FileInputStream("/home/martin/slovakia-nodes1.osm"));

		double lat = Double.NaN;
		double lon = Double.NaN;

		final List<Location> locations = new ArrayList<Location>();

		while (xer.hasNext()) {
			final XMLEvent event = xer.nextEvent();

			if (event.isStartElement()) {
				final StartElement startElement = event.asStartElement();
				if (startElement.getName().equals(NODE)) {
					lat = Double.parseDouble(startElement.getAttributeByName(LAT).getValue());
					lon = Double.parseDouble(startElement.getAttributeByName(LON).getValue());
				} else if (startElement.getName().equals(TAG) && startElement.getAttributeByName(K).getValue().equals("name")) {
					final Location location = new Location();
					location.lat = lat;
					location.lon = lon;
					location.name = startElement.getAttributeByName(V).getValue();
					location.existing = true;
					locations.add(location);
				}
			}
		}

		xer.close();

		return locations;
	}

}
