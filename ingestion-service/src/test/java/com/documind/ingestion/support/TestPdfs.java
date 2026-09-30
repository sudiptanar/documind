package com.documind.ingestion.support;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public final class TestPdfs {

    private TestPdfs() {
    }

    /** One page per entry, each page's lines written top-down. */
    public static byte[] pdf(List<List<String>> pages) {
        try (PDDocument doc = new PDDocument(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            PDType1Font font = new PDType1Font(Standard14Fonts.FontName.HELVETICA);
            for (List<String> lines : pages) {
                PDPage page = new PDPage();
                doc.addPage(page);
                try (PDPageContentStream cs = new PDPageContentStream(doc, page)) {
                    cs.beginText();
                    cs.setFont(font, 12);
                    cs.setLeading(16);
                    cs.newLineAtOffset(50, 720);
                    for (String line : lines) {
                        cs.showText(line);
                        cs.newLine();
                    }
                    cs.endText();
                }
            }
            doc.save(out);
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static byte[] contract() {
        return pdf(List.of(
                List.of("MASTER SERVICES AGREEMENT",
                        "This agreement is made between Acme Corp and Globex Ltd.",
                        "Section 9. Termination.",
                        "Either party may terminate this agreement with thirty (30) days written notice.",
                        "Termination does not affect accrued payment obligations."),
                List.of("Section 4. Fees and Payment.",
                        "Invoices are issued monthly and payment is due within forty-five (45) days.",
                        "Late payments accrue interest at one percent per month.")));
    }
}
