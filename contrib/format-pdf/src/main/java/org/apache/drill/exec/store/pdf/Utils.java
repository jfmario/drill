package org.apache.drill.exec.store.pdf;

import org.apache.pdfbox.pdmodel.PDDocument;
import technology.tabula.ObjectExtractor;
import technology.tabula.Page;
import technology.tabula.PageIterator;
import technology.tabula.Rectangle;
import technology.tabula.Table;
import technology.tabula.detectors.NurminenDetectionAlgorithm;
import technology.tabula.extractors.BasicExtractionAlgorithm;
import technology.tabula.extractors.ExtractionAlgorithm;
import technology.tabula.extractors.SpreadsheetExtractionAlgorithm;

import java.util.ArrayList;
import java.util.List;

public class Utils {

  public static List<Table> extractTablesFromPDF(PDDocument document) {
    NurminenDetectionAlgorithm detectionAlgorithm = new NurminenDetectionAlgorithm();

    ExtractionAlgorithm algExtractor;

    SpreadsheetExtractionAlgorithm extractor=new SpreadsheetExtractionAlgorithm();

    ObjectExtractor objectExtractor = new ObjectExtractor(document);
    PageIterator pages = objectExtractor.extract();
    List<Table> tables= new ArrayList<>();
    while (pages.hasNext()) {
      Page page = pages.next();
      if (extractor.isTabular(page)) {
        algExtractor=new SpreadsheetExtractionAlgorithm();
      }
      else {
        algExtractor = new BasicExtractionAlgorithm();
      }

      List<Rectangle> tablesOnPage = detectionAlgorithm.detect(page);

      for (Rectangle guessRect : tablesOnPage) {
        Page guess = page.getArea(guessRect);
        tables.addAll(algExtractor.extract(guess));
      }
    }
    return tables;
  }

}
