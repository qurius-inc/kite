/*
 * Copyright 2015 Qurius Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.uima.UIMAException;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.annotator.WhitespaceTokenizer;
import org.apache.uima.cas.impl.CASCompleteSerializer;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import java.io.*;
import java.util.*;

/**
 * Created by newton on 5/18/15.
 */
public class CasMorphlineTest extends AbstractMorphlineTest {

    // Create byte buffer that holds the a serialized test CAS Object
    public static JCas testCAS() throws UIMAException, IOException {
        String testString = "I like to travel through the Ether.";

        //TypeSystemDescription tsd = TypeSystemDescriptionFactory.createTypeSystemDescription("org.apache.uima.TokenAnnotation", "org.apache.uima.SentenceAnnotation");
        TypeSystemDescription tsd = TypeSystemDescriptionFactory.createTypeSystemDescription();
        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText(testString);
        jCas.setDocumentLanguage("en");
        jCas.createView("docId").setDocumentText("12345");

        AnalysisEngineDescription aed = AnalysisEngineFactory.createEngineDescription(WhitespaceTokenizer.class);
        AnalysisEngine ae = UIMAFramework.produceAnalysisEngine(aed);
        ae.process(jCas);

        //return Serialization.serializeCASComplete(jCas.getCasImpl());
        return jCas;
    }

    private void createTestSequenceFile(File file) throws UIMAException, IOException {
        FSDataOutputStream out = new FSDataOutputStream(new FileOutputStream(file), null);
        SequenceFile.Writer writer = null;
        SequenceFile.Metadata metadata = new SequenceFile.Metadata(getMetadataForSequenceFile());
        writer = SequenceFile.createWriter(new Configuration(), out, IntWritable.class, BytesWritable.class,
                SequenceFile.CompressionType.NONE, null, metadata);

        JCas jCas = testCAS();
        CASCompleteSerializer casCompleteSerializer = Serialization.serializeCASComplete(jCas.getCasImpl());
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(casCompleteSerializer);

        writer.append(new IntWritable(1), new BytesWritable(byteArrayOutputStream.toByteArray()));
    }

    private TreeMap<Text, Text> getMetadataForSequenceFile() {
        TreeMap<Text, Text> metadata = new TreeMap<Text, Text>();
        metadata.put(new Text("license"), new Text("Apache"));
        metadata.put(new Text("year"), new Text("2015"));
        return metadata;
    }


    @Test
    public void testCasRecord() throws Exception {
        morphline = createMorphline("test-morphlines/casRecord");

        Record record = new Record();
        record.put(Fields.ATTACHMENT_BODY, testCAS());

        collector.reset();
        startSession();
        assertEquals(1, collector.getNumStartEvents());
        assertTrue(morphline.process(record));
    }

    @Test
    public void testCasIndexer() throws Exception {
        morphline = createMorphline("test-morphlines/casIndexer");

        Record record = new Record();
        record.put(Fields.ATTACHMENT_BODY, testCAS());

        assertTrue(morphline.process(record));
        //Notifications.notifyCommitTransaction(morphline);
    }

    @Test
    public void testSeqCasIndexer() throws Exception {
        morphline = createMorphline("test-morphlines/seqCasIndexer");

        File sequenceFile = new File(RESOURCES_DIR, "test-data/test_cas.seq");
        createTestSequenceFile(sequenceFile);
        //FileInputStream in = new FileInputStream(sequenceFile.getAbsolutePath());
        FileInputStream in = new FileInputStream(new File("src/test/resources/test-data/nyt_head_5.seq"));
        Record record = new Record();
        record.put(Fields.ATTACHMENT_BODY, in);
        startSession();

        assertEquals(1, collector.getNumStartEvents());
        assertTrue(morphline.process(record));
    }
}
