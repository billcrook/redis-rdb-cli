package com.moilioncircle.redis.rdb.cli.ext.rct;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.glossary.Escape;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.Replicator;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.List;


public class JsonlKeyedHashExplodeRdbVisitor extends JsonlHashExplodeRdbVisitor {

    public JsonlKeyedHashExplodeRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types, Escape escape) {
        super(replicator, configure, out, db, regexs, types, escape);
    }

    protected void emitHashJson(byte[] key, byte[] field, byte[] value) throws java.io.IOException {
        OutputStreams.write('{', out);
        byte[] sep = ":".getBytes();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(key);
        bos.write(sep);
        bos.write(field);
        byte[] newKey = bos.toByteArray();
        emitField("key", newKey);

        OutputStreams.write(',', out);
        emitField("value", value);

        OutputStreams.write('}', out);
    }

}
