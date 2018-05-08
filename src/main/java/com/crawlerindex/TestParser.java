package com.crawlerindex;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import javax.measure.unit.SystemOfUnits;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public class TestParser extends AbstractParser {
    public Set<MediaType> getSupportedTypes(ParseContext parseContext) {
        return null;
    }

    public void parse(InputStream inputStream, ContentHandler contentHandler, Metadata metadata, ParseContext parseContext) throws IOException, SAXException, TikaException {

        System.out.println(metadata.get("KEYWORDS"));

    }
}
