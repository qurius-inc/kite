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

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import com.typesafe.config.Config;
import org.apache.hadoop.io.BytesWritable;
import org.apache.uima.cas.impl.CASCompleteSerializer;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by newton on 5/22/15.
 */
public class casDeserializer implements CommandBuilder {

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("casDeserializer");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context){
        return new CasDeserializer(this, config, parent, child, context);
    }


    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static final class CasDeserializer extends AbstractCommand {
        private String casField;

        public CasDeserializer(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
            casField = getConfigs().getString(config, "casField", null);
        }

       @Override
       protected boolean doProcess(Record inputRecord) {
           try {
               BytesWritable bytesWritable = (BytesWritable) inputRecord.getFirstValue(casField);
               ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytesWritable.getBytes());
               ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
               CASCompleteSerializer casCompleteSerializer = (CASCompleteSerializer) objectInputStream.readObject();

               Record outputRecord = new Record();
               outputRecord.put(Fields.ATTACHMENT_BODY, casCompleteSerializer);
               getChild().process(outputRecord);
           } catch (Exception e) {
               return false;
           }
           return true;
       }
    }

}
