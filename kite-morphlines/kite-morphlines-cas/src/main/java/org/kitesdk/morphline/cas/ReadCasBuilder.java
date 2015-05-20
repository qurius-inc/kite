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
package org.kitesdk.morphline.cas;

import com.typesafe.config.Config;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.admin.CASMgr;
import org.apache.uima.cas.impl.AnnotationImpl;
import org.apache.uima.cas.impl.CASCompleteSerializer;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.stdio.AbstractParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Collections;

/**
 * Command that parses an InputStream that contains CAS data; for each CAS object in the stream,
 * the command emits a morphline record containing the object as an attachment in
 */
public final class ReadCasBuilder implements CommandBuilder {

    /** The MIME type identifier that will be filled into output records */
    public static final String MIME_TYPE = "cas";

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("readCas");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new ReadCas(this, config, parent, child, context);
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static final class ReadCas extends AbstractParser {

        public ReadCas (CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
        }

        @Override
        protected boolean doProcess(Record inputRecord, InputStream in) throws IOException {
            ObjectInputStream oins = (ObjectInputStream) in;
            Object obj = null;
            CASCompleteSerializer casCompleteSerializer = null;
            JCas jCas = null;

            try {
                obj = oins.readObject();
            } catch (ClassNotFoundException e) {
               return false;
            }

            if (obj instanceof CASCompleteSerializer) {
                casCompleteSerializer = (CASCompleteSerializer) obj;
            } else {
                throw new IOException("Deserialized Object is not an instance of CASCompleteSerializer");
            }

            try {
                 jCas = JCasFactory.createJCas();
                CASMgr casMgr = jCas.getCasImpl();
                Serialization.deserializeCASComplete(casCompleteSerializer, casMgr);

                String content = jCas.getDocumentText();
                System.out.println(" ** Main Content ** \n" + content);
            } catch (UIMAException e) {
                return false;
            }

            /* Check the Annotations */
            FSIterator iterator = jCas.getAnnotationIndex().iterator();
            while (iterator.hasNext()) {
                AnnotationImpl annotation = (AnnotationImpl) iterator.next();
                System.out.print("\n *** " + annotation.getCoveredText() + "\n");
                System.out.print(annotation.toString());
            }

            return true;
        }
    }

}