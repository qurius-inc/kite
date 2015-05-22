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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.uima.TokenAnnotation;
import org.apache.uima.UIMAException;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.annotator.WhitespaceTokenizer;
import org.apache.uima.cas.admin.CASMgr;
import org.apache.uima.cas.impl.CASCompleteSerializer;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by newton on 5/18/15.
 */
public class CasMorphlineTest extends AbstractMorphlineTest {

    // Create byte buffer that holds the a serialized test CAS Object
    public static CASCompleteSerializer testCAS() throws UIMAException, IOException {
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

        return Serialization.serializeCASComplete(jCas.getCasImpl());
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

        morphline.process(record);
        Notifications.notifyCommitTransaction(morphline);
    }

}
