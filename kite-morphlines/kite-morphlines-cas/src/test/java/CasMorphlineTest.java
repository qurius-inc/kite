import org.apache.uima.UIMAException;
import org.apache.uima.cas.admin.CASMgr;
import org.apache.uima.cas.impl.CASCompleteSerializer;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import java.io.*;
import java.util.Iterator;

/**
 * Created by newton on 5/18/15.
 */
public class CasMorphlineTest extends AbstractMorphlineTest {

    // Create byte buffer that holds the a serialized test CAS Object
    public static byte[] testByteArrayFactory() throws UIMAException, IOException {
        String testString = "I like to travel through the Ether";

        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText(testString);
        jCas.setDocumentLanguage("en");
        jCas.createView("docId").setDocumentText("12345");

        Annotation ann = new Annotation(jCas);
        // set "I"
        ann.setBegin(0);
        ann.setEnd(1);
        ann.addToIndexes();
        // set "travel"
        ann = new Annotation(jCas);
        ann.setBegin(10);
        ann.setEnd(16);
        ann.addToIndexes();
        // set "Ether"
        ann = new Annotation(jCas);
        ann.setBegin(29);
        ann.setEnd(34);
        ann.addToIndexes();

        CASCompleteSerializer casCompleteSerializer = Serialization.serializeCASComplete(jCas.getCasImpl());
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(casCompleteSerializer);


        return byteArrayOutputStream.toByteArray();
    }

    @Test
    public void testReadCas() throws Exception {
        morphline = createMorphline("test-morphlines/readJson");
        for (int i = 0; i < 3; i++) {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(testByteArrayFactory());
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            Record record = new Record();
            record.put(Fields.ATTACHMENT_BODY, objectInputStream);

            collector.reset();
            startSession();
            assertEquals(1, collector.getNumStartEvents());
            assertTrue(morphline.process(record));
            Iterator<Record> iter = collector.getRecords().iterator();

            objectInputStream.close();
        }
    }

    @Test
}
