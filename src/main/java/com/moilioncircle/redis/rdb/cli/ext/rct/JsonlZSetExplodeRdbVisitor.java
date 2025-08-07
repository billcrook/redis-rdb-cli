package com.moilioncircle.redis.rdb.cli.ext.rct;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.glossary.Escape;
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


public class JsonlZSetExplodeRdbVisitor extends JsonlRdbVisitor {

    public JsonlZSetExplodeRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types, Escape escape) {
        super(replicator, configure, out, db, regexs, types, escape);
    }

    protected void emitZSetJson( byte[] key, byte[] element, double score){
        Outputs.write('{', out);
        emitField("key", key);

        Outputs.write(',', out);
        emitString("member".getBytes());
        Outputs.write(':', out);
        emitString(element);

        Outputs.write(',', out);
        emitString("score".getBytes());
        Outputs.write(':', out);
        emitString(String.valueOf(score).getBytes());

        Outputs.write('}', out);
    }

    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {

        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;

        while (len > 0) {

            byte[] element = parser.rdbLoadEncodedStringObject().first();

            double score = parser.rdbLoadBinaryDoubleValue();

            if (!firstkey) {
                separator();
            }

            firstkey = false;

            emitZSetJson(key, element, score);

            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {

            if (!firstkey) {
                separator();
            }

            firstkey = false;
            byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
            zllen--;
            emitZSetJson(key, element, score);
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyHash, key: " + new String(key));
    }

    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyHashZipList, key: " + new String(key));
    }

    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyString, key: " + new String(key));
    }

    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyList, key: " + new String(key));
    }

    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplySet, key: " + new String(key));
    }

    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyZSet, key: " + new String(key));
    }

    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyHashZipMap, key: " + new String(key));
    }

    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyListZipList, key: "+new String(key));
    }

    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplySetIntSet, key: " + new String(key));
    }

    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyListQuickList, key: " + new String(key));
    }

    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyModule, key: " + new String(key));
    }

    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyModule2, key: " + new String(key));
    }

    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        throw new UnsupportedOperationException("doApplyStreamListPacks, key: " + new String(key));
    }

}
