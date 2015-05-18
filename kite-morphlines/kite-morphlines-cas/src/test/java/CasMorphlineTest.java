import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Created by newton on 5/18/15.
 */
public class CasMorphlineTest extends AbstractMorphlineTest {

    @Test
    public void testReadCas() throws Exception {
        morphline = createMorphline("test-morphlines/readCas");
        for (int i = 0; i < 3; i++) {
            InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-cas/test.cas"));
            Record record = new Record();
            record.put(Fields.ATTACHMENT_BODY, in);

            collector.reset();
            startSession();
            assertEquals(1, collector.getNumStartEvents());
            assertTrue(morphline.process(record));
            Iterator<Record> iter = collector.getRecords().iterator();


        }
    }

}
