package com.crawlerindex;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.ToHTMLContentHandler;
import org.jwat.warc.WarcRecord;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class ProcessWarc implements Callable<Keywords> {

	WarcRecord record;
	int numRecords;
	PreparedStatement indexKeywords;
	PreparedStatement getUrl;

	List<String> urlList = new ArrayList<String>();


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

	@Override
	public Keywords call() throws Exception {

		try {
			Metadata metadata = pharseHtml(record.getPayload().getInputStreamComplete());

			if ((++numRecords) % 1000 == 0) {
				urlList.clear();
			}

			String uri = null;
			if (record.header.warcTargetUriUri != null) {
				uri = record.header.warcTargetUriUri.toString();
			} else {
				return null;
			}

			if (uri.contains(".com/")) {
				int i = uri.indexOf(".com/");
				if (i + 5 != uri.length()) {
					return null;
				}
			}

			String simplyfiedUrl = simplfyUrl(uri);

			if (simplyfiedUrl == null) {
				return null;
			}

			if (isUrlAlreadyIndexed(simplyfiedUrl, getUrl) || isUrlAlreadyIndexedtoList(simplyfiedUrl, urlList)) {
				return null;
			}

			String keywords = null;
			String title = null;
			if (metadata.get("KEYWORDS") != null) {
				keywords = metadata.get("KEYWORDS");
			} else if (metadata.get("keywords") != null) {
				keywords = metadata.get("keywords");
			}

			if (keywords == null || keywords.length() > 5000) {
				return null;
			}

			if (metadata.get("dc:title") != null) {
				title = metadata.get("dc:title");
			} else if (metadata.get("title") != null) {
				title = metadata.get("title");
			}

			if (title.length() > 500) {
				return null;
			}

			Keywords keywords1 = new Keywords();
			keywords1.setUrl(simplyfiedUrl);
			keywords1.setTitle(title);
			keywords1.setUrl(keywords);

			return keywords1;

		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TikaException e) {
			e.printStackTrace();
		}

		return null;
	}

}
