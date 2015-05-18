package org.qurius.morphline.cas;

import com.typesafe.config.Config;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.stdio.AbstractParser;

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

/**
 * Command that parses an InputStream that contains JSON data; for each JSON object in the stream,
 * the command emits a morphline record containing the object as an attachment in
 * {@link Fields#ATTACHMENT_BODY}.
 */
public class ReadCasBuilder extends CommandBuilder {

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("readCas");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////

    private static final class ReadCas extends AbstractParser {
        @Override
        protected boolean doProcess(Record inputRecord, InputStream in) {


        }
    }

}