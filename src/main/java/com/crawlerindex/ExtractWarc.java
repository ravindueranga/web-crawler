package com.crawlerindex;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.ToHTMLContentHandler;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ExtractWarc {

	final static String WARCName = "D://CC-MAIN-20151124205405-00311-ip-10-71-132-137.ec2.internal.warc.gz";
	final static String outputDir = "D://Output Directory";
	final static String sql = "insert into z3.keywords (url, title, keywords) values (?,?,?)";
	final static String getUrlSql = "select * from z3.keywords where url=?";

	private static boolean isUrlAlreadyIndexed(String simplyfiedUrl, PreparedStatement getUrl) {

		try {
			getUrl.setString(1, simplyfiedUrl);
			ResultSet resultSet = getUrl.executeQuery();
			if (resultSet.next()) {
				return true;
			} else {
				return false;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return true;
	}

	private static boolean isUrlAlreadyIndexedtoList(String simplyfiedUrl, List<String> urlList) {

		if (urlList.contains(simplyfiedUrl)) {
			return true;

		} else {
			return false;
		}
	}

	private static String simplfyUrl(String url) {

		if (url.contains(".com/")) {
			String s = url.substring(0, url.indexOf(".com/") + 4);
			return s;
		} else {
			return null;
		}
	}

	static File createOutputDirectory(String name) {

		File storeDir = new File(name);
		if (storeDir.exists())
			System.out.println("Warning: " + name + " directory already exists");
		else
			storeDir.mkdir();
		return storeDir;
	}

	private static Metadata pharseHtml(InputStream inputStream) throws TikaException, SAXException, IOException {

		LinkContentHandler linkHandler = new LinkContentHandler();
		ContentHandler textHandler = new BodyContentHandler(-1);
		ToHTMLContentHandler toHTMLHandler = new ToHTMLContentHandler();
		TeeContentHandler teeHandler = new TeeContentHandler(linkHandler, textHandler, toHTMLHandler);
		Metadata metadata = new Metadata();
		ParseContext parseContext = new ParseContext();
		HtmlParser parser = new HtmlParser();
		parser.parse(inputStream, teeHandler, metadata, parseContext);

		return metadata;
	}

	private static Connection getConnection() {

		Connection con = null;

		//Database info
		String url = "";
		String user = "";
		String password = "";

		try {
			con = DriverManager.getConnection(url, user, password);

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return con;
	}

	public void process(InputStream inputFile) throws SQLException {

		Connection connection = getConnection();
		PreparedStatement indexKeywords = connection.prepareStatement(sql);
		PreparedStatement getUrl = connection.prepareStatement(getUrlSql);

		try {
			WarcReader reader = WarcReaderFactory.getReader(inputFile);
			WarcRecord record;
			long numRecords = 0;
			List<String> urlList = new ArrayList<String>();

			while ((record = reader.getNextRecord()) != null) {

				if ((++numRecords) % 100000 == 0) {

					System.out.println(numRecords + " records reached");
				}

				if (urlList.size() == 50) {
					indexKeywords.executeBatch();
					indexKeywords.clearBatch();
					urlList.clear();
				}

				String uri = null;
				if (record.header.warcTargetUriUri != null) {
					uri = record.header.warcTargetUriUri.toString();
				} else {
					continue;
				}

				if (uri.contains(".com/")) {
					int i = uri.indexOf(".com/");
					if (i + 5 != uri.length()) {
						continue;
					}
				}

				String simplyfiedUrl = simplfyUrl(uri);

				if (simplyfiedUrl == null) {
					continue;
				}

				if (isUrlAlreadyIndexed(simplyfiedUrl, getUrl) || isUrlAlreadyIndexedtoList(simplyfiedUrl, urlList)) {
					continue;
				}

				urlList.add(simplyfiedUrl);

				Metadata metadata = pharseHtml(record.getPayload().getInputStreamComplete());

				String keywords = null;
				String title = null;
				if (metadata.get("KEYWORDS") != null) {
					keywords = metadata.get("KEYWORDS");
				} else if (metadata.get("keywords") != null) {
					keywords = metadata.get("keywords");
				}

				if (keywords == null || keywords.length() > 5000) {
					continue;
				}

				if (metadata.get("dc:title") != null) {
					title = metadata.get("dc:title");
				} else if (metadata.get("title") != null) {
					title = metadata.get("title");
				}

				if (title.length() > 500) {
					continue;
				}

				System.out.println(simplyfiedUrl);
				indexKeywords.setString(1, simplyfiedUrl);
				indexKeywords.setString(2, title);
				indexKeywords.setString(3, keywords);

				indexKeywords.addBatch();

			}

			if (!urlList.isEmpty()) {
				indexKeywords.executeBatch();
				indexKeywords.clearBatch();
				urlList.clear();
			}

		} catch (IOException e1) {
			System.out.println("IO Exception Occured " + e1);
		} catch (TikaException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} finally {
			connection.close();
		}

	}

}
