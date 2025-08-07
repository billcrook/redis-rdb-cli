package com.moilioncircle.redis.rdb.cli.ext.rct;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.cmd.Args;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.File;
import java.io.IOException;
import java.util.List;


public class JsonlZSetRdbVisitor extends JsonlRdbVisitor {

    public JsonlZSetRdbVisitor(Replicator replicator, Configure configure, Args.RctArgs args, Escaper escaper) {
        super(replicator, configure, args, escaper);
    }

    /* override to produce json with member and score attributes */
    protected void emitZSet(byte[] field, double value) {
        Outputs.write('{', out);
        emitString("member".getBytes());
        Outputs.write(':', out);
        emitString(field);

        Outputs.write(',', out);
        emitString("score".getBytes());
        Outputs.write(':', out);
        emitString(String.valueOf(value).getBytes());

        Outputs.write('}', out);
    }

    /* override to produce an array of member/score elements */
    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            Outputs.write('[', out); // CHANGED HERE
            BaseRdbParser parser = new BaseRdbParser(in);
            long len = parser.rdbLoadLen().len;
            boolean flag = true;
            while (len > 0) {
                if (!flag) {
                    Outputs.write(',', out);
                }
                byte[] element = parser.rdbLoadEncodedStringObject().first();
                double score = parser.rdbLoadDoubleValue();
                emitZSet(element, score);
                flag = false;
                len--;
            }
            Outputs.write(']', out); // CHANGED HERE
        });
        return context.valueOf(new DummyKeyValuePair());
    }

    /* override to produce an array of member/score elements */
    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            Outputs.write('[', out); // CHANGED HERE
            BaseRdbParser parser = new BaseRdbParser(in);
            long len = parser.rdbLoadLen().len;
            boolean flag = true;
            while (len > 0) {
                if (!flag) {
                    Outputs.write(',', out);
                }
                byte[] element = parser.rdbLoadEncodedStringObject().first();
                double score = parser.rdbLoadBinaryDoubleValue();
                emitZSet(element, score);
                flag = false;
                len--;
            }
            Outputs.write(']', out); // CHANGED HERE
        });
        return context.valueOf(new DummyKeyValuePair());
    }

    /* override to produce an array of member/score elements */
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            Outputs.write('[', out); // CHANGED HERE
            BaseRdbParser parser = new BaseRdbParser(in);
            RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
            boolean flag = true;
            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            while (zllen > 0) {
                if (!flag) {
                    Outputs.write(',', out);
                }
                byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
                zllen--;
                double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
                zllen--;
                emitZSet(element, score);
                flag = false;
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
            Outputs.write(']', out); // CHANGED HERE
        });
        return context.valueOf(new DummyKeyValuePair());
    }
}
