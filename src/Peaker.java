import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.postgresql.Driver;
import org.postgresql.jdbc4.Jdbc4Connection;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


// CREATE INDEX idx_1 on nodes ( (tags->'natural') );
// CREATE INDEX idx_2 on nodes ( (tags->'name') );

public class Peaker {

//	private static final int ELE_OFFSET = 3; // add this to computed ele because of interpolation
	private static final int SAME_PEAK_SKIP_DISTANCE = 100;
	private static final int SAME_PEAK_SKIP_DISTANCE_DB = 500;
	private static final int PEAK_ELE_TOLERANCE = 10;
	private static final int WEIGHT_LIMIT = 3000; // 2300, 5000
	private static final int CACHE_SIZE = 4;
	private static final int INSPECTED_AREA_HALF_SIDE = 8; // was 4
	// private static final String HGT_DIR = "/media/martin/data/martin/mapping/srtm-all/";
	//private static final int RESOLUTION = 1200; // SRTM3
	private static final String HGT_DIR = "/media/martin/data/martin/mapping/dmr20/";
	private static final int RESOLUTION = 3600; // SRTM1

	private static final double STEP = 1.0 / RESOLUTION; // SRTM1

	private final int[] lats = new int[CACHE_SIZE], lons = new int[CACHE_SIZE];
	private final ShortBuffer[] sbs = new ShortBuffer[CACHE_SIZE];
	private int ii;

	private static final SAXParserFactory factory = SAXParserFactory.newInstance();
	private static SAXParser saxParser;

	private static class Peak {
		final String name;
		final int ele;
		final double minLat;
		final double minLon;
		final double maxLat;
		final double maxLon;

		List<Double> lats;
		List<Double> lons;
		List<Double> eles;
		public File file;

		public Peak(final String name, final int ele, final double minLat, final double minLon, final double maxLat, final double maxLon, final File file) {
			this.name = name;
			this.ele = ele;
			this.minLat = minLat;
			this.minLon = minLon;
			this.maxLat = maxLat;
			this.maxLon = maxLon;
			this.file = file;
		}
	}

	private static class PeakCandidate implements Serializable {
		private static final long serialVersionUID = 2450281635647464848L;
		double lat;
		double lon;
		double ele;
	}

	public static void main(final String[] args) throws IOException, ParserConfigurationException, SAXException, SQLException {
		saxParser = factory.newSAXParser();

		final List<Peak> peakList = new ArrayList<>();

		for (final File file : new File("peaks").listFiles()) {
			parse(peakList, file);
		}

		new Peaker().run(peakList);
	}

	private static void parse(final List<Peak> peakList, final File file) throws SAXException, IOException, FileNotFoundException {
		try (final FileInputStream is = new FileInputStream(file)) {
			saxParser.parse(is, new DefaultHandler() {
				String path = "";
				String name;
				int ele = Integer.MIN_VALUE;
				double minLat = Double.MAX_VALUE;
				double minLon = Double.MAX_VALUE;
				double maxLat = Double.MIN_VALUE;
				double maxLon = Double.MIN_VALUE;

				@Override
				public void startElement(final String uri, final String localName, final String qName, final Attributes attributes) throws SAXException {
					path = path + "/" + qName;

					if (path.equals("/osm/node")) {
						final double lat = Double.parseDouble(attributes.getValue("lat"));
						final double lon = Double.parseDouble(attributes.getValue("lon"));
						minLat = Math.min(minLat, lat);
						minLon = Math.min(minLon, lon);
						maxLat = Math.max(maxLat, lat);
						maxLon = Math.max(maxLon, lon);
					} else if (path.equals("/osm/node/tag")) {
						final String key = attributes.getValue("k");
						if ("name".equals(key)) {
							name = attributes.getValue("v");
						} else if ("ele".equals(key)) {
							final String eleString = attributes.getValue("v");
							if (!eleString.isEmpty()) {
								try {
									ele = Integer.parseInt(eleString.replaceAll("(.*)m", "\\1"));
								} catch (final NumberFormatException e) {
									System.out.println("invalid elevation " + eleString + " in " + file);
								}
							}
						}
					}
				}

				@Override
				public void endElement(final String uri, final String localName, final String qName) throws SAXException {
					path = path.substring(0, path.lastIndexOf('/'));
				}

				@Override
				public void endDocument() throws SAXException {
					if (ele != Integer.MIN_VALUE) {
						peakList.add(new Peak(name, ele, minLat, minLon, maxLat, maxLon, file));
					} else {
						System.out.println("Missing elevation in " + file);
					}
				}
			});
		}
	}

	private void run(final List<Peak> peakList) throws IOException, SQLException {
		int id = 0;

		final Properties properties = new Properties();
		properties.setProperty("user", "martin");
		properties.setProperty("password", "b0n0");
		final Connection connection = new Driver().connect("jdbc:postgresql://localhost:5432/gis", properties);
		((Jdbc4Connection) connection).addDataType("geometry", "org.postgis.PGgeometry");
		((Jdbc4Connection) connection).addDataType("box3d", "org.postgis.PGbox3d");

		final double minLat = alignToStep(47.7);
		final double maxLat = alignToStep(49.64);

		final double minLon = alignToStep(16.8);
		final double maxLon = alignToStep(22.57);

//		final double minLat = alignToStep(48.8);
//		final double maxLat = alignToStep(49.0);
//
//		final double minLon = alignToStep(19);
//		final double maxLon = alignToStep(20);

		final List<PeakCandidate> pcList = new ArrayList<>();

		final BicubicInterpolator bci = new BicubicInterpolator();

		int oldPct = -1;
		int pct;
		for (double lat = minLat; lat < maxLat; lat += STEP) {
			pct = (int) ((lat - minLat) / (maxLat - minLat) * 100.0);
			if (oldPct != pct) {
				System.out.println("----------> " + pct + "%");
				oldPct = pct;
			}

			// find peak candidates
			for (double lon = minLon; lon < maxLon; lon += STEP) {
				final int h1 = getHeight(lat, lon);

				found: {
					int sum = 0;
					for (int x = -INSPECTED_AREA_HALF_SIDE; x <= INSPECTED_AREA_HALF_SIDE; x++) {
						for (int y = -INSPECTED_AREA_HALF_SIDE; y <= INSPECTED_AREA_HALF_SIDE; y++) {
							if (x != y) {
								final short h2 = getHeight(lat + STEP * x, lon + STEP * y);
								if (h1 < h2) {
									break found;
								}

//								final int xx = INSPECTED_AREA_HALF_SIDE - Math.abs(x);
//								final int yy = INSPECTED_AREA_HALF_SIDE - Math.abs(y);
								sum += /*Math.sqrt(xx * xx + yy * yy) **/ h1 - h2;
							}
						}
					}

					if (sum > WEIGHT_LIMIT) {
						System.out.println("GOT:" + sum);
						double h = Double.MIN_VALUE;

						double lat1 = Double.NaN, lon1 = Double.NaN;

						for (int x = -INSPECTED_AREA_HALF_SIDE; x <= INSPECTED_AREA_HALF_SIDE - 4; x++) {
							for (int y = -INSPECTED_AREA_HALF_SIDE; y <= INSPECTED_AREA_HALF_SIDE - 4; y++) {
								final double[][] p = new double[4][4];
								for (int xx = 0; xx < 4; xx++) {
									for (int yy = 0; yy < 4; yy++) {
										p[xx][yy] = getHeight(lat + STEP * (x + xx), lon + STEP * (y + yy));
									}
								}

								for (double a = 0.0; a <= 1.0; a += 0.2) {
									for (double b = 0.0; b <= 1.0; b += 0.2) {
										final double hh = bci.getValue(p, a, b);
										if (h < hh) {
											h = hh;
											lat1 = lat + STEP * a;
											lon1 = lon + STEP * b;
										}
									}
								}
							}
						}

						final PeakCandidate pc = new PeakCandidate();
						pc.ele = h;
						pc.lat = lat1;
						pc.lon = lon1;
						pcList.add(pc);
					}
				}
			}
		}

		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("/home/martin/pcs"))) {
			oos.writeObject(pcList);
		}

		// filter near peak candidates

		final int size = pcList.size();
		for (int i = 0; i < size - 1; i++) {
			for (int j = i + 1; j < size; j++) {
				final PeakCandidate pc1 = pcList.get(i);
				final PeakCandidate pc2 = pcList.get(j);
				if (pc1 != null && pc2 != null && distFrom(pc1.lat, pc1.lon, pc2.lat, pc2.lon) < SAME_PEAK_SKIP_DISTANCE) {
					if (pc1.ele < pc2.ele) {
						pcList.set(i, null);
					} else {
						pcList.set(j, null);
					}
				}
			}
		}

		int removed = 0;
		for (final Iterator<PeakCandidate> iter = pcList.iterator(); iter.hasNext();) {
			if (iter.next() == null) {
				iter.remove();
				removed++;
			}
		}
		System.out.println("Removed: " + removed);

		final FileWriter fw = new FileWriter("peaks.osm");

		fw.write("<?xml version='1.0' encoding='UTF-8'?>");
		fw.write("<osm version='0.6'>");

		for (final PeakCandidate pc : pcList) {

//			fw.write(String.format("<node id='%d' lat='%f' lon='%f'/>", --id, lat, lon));
//			if (true) {
//				continue;
//			}

			peak: for (final Peak peak : peakList) {
				if (peak.file != null && peak.minLat <= pc.lat && peak.maxLat >= pc.lat && peak.minLon <= pc.lon && peak.maxLon >= pc.lon
						&& Math.abs(pc.ele - peak.ele) < PEAK_ELE_TOLERANCE) {

					if (true) {
						final Statement stmt = connection.createStatement();
						final String sql = "SELECT id FROM nodes WHERE tags->'natural' = 'peak' AND tags->'name' = '" + peak.name + "' "
								+ "AND ST_DWithin(geom, ST_SetSRID(ST_Point(" + pc.lon + ", " + pc.lat + "), 4326), " + SAME_PEAK_SKIP_DISTANCE_DB + ")";
						final ResultSet rs = stmt.executeQuery(sql);
						if (rs.next()) {
					    	//final org.postgis.Point point = (org.postgis.Point) ((PGgeometry) rs.getObject(1)).getGeometry();
					    	//System.out.println(rs.getString(2) + " " + point.getX() + " " + point.getY());
							System.out.println("Already: " + peak.name);
							peak.file = null; // XXX mark as ignored
							continue;
						}
						rs.close();
						stmt.close();
					}

					if (peak.lats == null) {
						peak.lats = new ArrayList<>(2);
						peak.lons = new ArrayList<>(2);
						peak.eles = new ArrayList<>(2);
					} else {
//						final int n = peak.lats.size();
//						for (int i = 0; i < n; i++) {
//							final double distFrom = distFrom(peak.lats.get(i), peak.lons.get(i), pc.lat, pc.lon);
//							if (distFrom < SAME_PEAK_SKIP_DISTANCE) {
//								System.out.println("Ambig: " + peak.name + "; dist: " + distFrom);
//								continue peak;
//							}
//						}
					}

					peak.lats.add(pc.lat);
					peak.lons.add(pc.lon);
					peak.eles.add(pc.ele);
				}
			}
		}

		connection.close();


		for (final Peak peak : peakList) {
			if (peak.lats != null && peak.file != null) {
				final int n = peak.lats.size();
				for (int i = 0; i < n; i++) {
					fw.write(String.format("<node id='%d' lat='%f' lon='%f'>"
							+ "<tag k='natural' v='peak'/>"
							+ "<tag k='import_ref' v='Záväzné názvoslovie vrchov'/>"
							+ "<tag k='source:name' v='skgeodesy.sk'/>"
							+ "<tag k='name' v='%s'/>"
							+ "<tag k='ele' v='%d'/>"
							+ "<tag k='x-dem-ele' v='%f'/>"
							+ "%s"
							+ "</node>\n", --id, peak.lats.get(i), peak.lons.get(i), peak.name, peak.ele, peak.eles.get(i), n > 1 ? "<tag k='fixme' v='ambiguous'/>" : ""));
				}
			}
		}


		fw.write("</osm>");

		fw.close();

		for (final Peak peak : peakList) {
			if (peak.lats == null) {
				System.out.println("No match: " + peak.file);
			}
		}

	}

	private short getHeight(final double lat, final double lon) throws IOException, FileNotFoundException {
		final ShortBuffer sb = getShortBuffer(lat, lon);

		final int latFrag = (int) Math.round((lat - Math.floor(lat)) * RESOLUTION);
		final int lonFrag = (int) Math.round((lon - Math.floor(lon)) * RESOLUTION);

		return sb.get((RESOLUTION - latFrag) * (RESOLUTION + 1) + lonFrag);
	}

	private ShortBuffer getShortBuffer(final double lat, final double lon) throws IOException, FileNotFoundException {
		for (int i = 0; i < 2; i++) {
			if (lats[i] == (int) lat && lons[i] == (int) lon) {
				return sbs[i];
			}
		}

		ii = (ii + 1) % CACHE_SIZE;

		final ByteBuffer bb;
		try (FileInputStream is = new FileInputStream(String.format(HGT_DIR + "N%02dE%03d.hgt", (int) lat, (int) lon))) {
			final FileChannel fc = is.getChannel();
			bb = ByteBuffer.allocateDirect((int) fc.size());
			while (bb.remaining() > 0) {
				fc.read(bb);
			}
		}
		bb.flip();

		lats[ii] = (int) lat;
		lons[ii] = (int) lon;

		sbs[ii] = bb.order(ByteOrder.BIG_ENDIAN).asShortBuffer();
		return sbs[ii];
	}

	public static double distFrom(final double lat1, final double lng1, final double lat2, final double lng2) {
		final double earthRadius = 3958.75;
		final double dLat = Math.toRadians(lat2 - lat1);
		final double dLng = Math.toRadians(lng2 - lng1);
		final double sindLat = Math.sin(dLat / 2);
		final double sindLng = Math.sin(dLng / 2);
		final double a = Math.pow(sindLat, 2) + Math.pow(sindLng, 2) * Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2));
		final double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		final double dist = earthRadius * c;

		return dist * 1609.344;
	}

	private static double alignToStep(final double d) {
		return Math.round(d / STEP) * STEP;
	}

	public class CubicInterpolator {
		public double getValue(final double[] p, final double x) {
			return p[1] + 0.5 * x * (p[2] - p[0] + x * (2.0 * p[0] - 5.0 * p[1] + 4.0 * p[2] - p[3] + x * (3.0 * (p[1] - p[2]) + p[3] - p[0])));
		}
	}

	public class BicubicInterpolator extends CubicInterpolator {
		private final double[] arr = new double[4];

		public double getValue(final double[][] p, final double x, final double y) {
			arr[0] = getValue(p[0], y);
			arr[1] = getValue(p[1], y);
			arr[2] = getValue(p[2], y);
			arr[3] = getValue(p[3], y);
			return getValue(arr, x);
		}
	}

	public class TricubicInterpolator extends BicubicInterpolator {
		private final double[] arr = new double[4];

		public double getValue(final double[][][] p, final double x, final double y, final double z) {
			arr[0] = getValue(p[0], y, z);
			arr[1] = getValue(p[1], y, z);
			arr[2] = getValue(p[2], y, z);
			arr[3] = getValue(p[3], y, z);
			return getValue(arr, x);
		}
	}

}
