package com.moilioncircle.redis.rdb.cli.ext.rct;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.cmd.Args;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.replicator.Replicator;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.List;


public class JsonlKeyedHashExplodeRdbVisitor extends JsonlHashExplodeRdbVisitor {

    public JsonlKeyedHashExplodeRdbVisitor(Replicator replicator, Configure configure, Args.RctArgs args, Escaper escaper) {
        super(replicator, configure, args, escaper);
    }

    protected void emitHashJson(byte[] key, byte[] field, byte[] value) throws java.io.IOException {
        Outputs.write('{', out);
        byte[] sep = ":".getBytes();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(key);
        bos.write(sep);
        bos.write(field);
        byte[] newKey = bos.toByteArray();
        emitField("key", newKey);

        Outputs.write(',', out);
        emitField("value", value);

        Outputs.write('}', out);
    }

}
