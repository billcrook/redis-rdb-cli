/*
 * Copyright 2018-2019 Baoyi Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.rdb.cli.glossary;

import java.io.File;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.rct.*;
import com.moilioncircle.redis.replicator.Replicator;

/**
 * @author Baoyi Chen
 */
public enum Format {
    KEY("key"),
    MEM("mem"),
    DUMP("dump"),
    DIFF("diff"),
    JSON("json"),
    RESP("resp"),
    JSONL("jsonl"),
    JSONLBZ2("jsonlbz2"),
    JSONLZSETEXPLODE("jsonlzsetexplode"),
    JSONLHASHEXPLODE("jsonlhashexplode"),
    JSONLKEYEDHASHEXPLODE("jsonlkeyedhashexplode"),
    COUNT("count"),
    KEYVAL("keyval");

    private String value;

    Format(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static Format parse(String format) {
        switch (format) {
            case "key":
                return KEY;
            case "mem":
                return MEM;
            case "dump":
                return DUMP;
            case "diff":
                return DIFF;
            case "json":
                return JSON;
            case "resp":
                return RESP;
            case "jsonl":
                return JSONL;
            case "jsonlbz2":
                return JSONLBZ2;
            case "jsonlzsetexplode":
                return JSONLZSETEXPLODE;
            case "jsonlhashexplode":
                return JSONLHASHEXPLODE;
            case "jsonlkeyedhashexplode":
                return JSONLKEYEDHASHEXPLODE;
            case "count":
                return COUNT;
            case "keyval":
                return KEYVAL;
            default:
                throw new AssertionError("Unsupported format '" + format + "'");
        }
    }

    public void dress(Replicator r, Configure conf, File output, List<Long> db, List<String> regexs, Long largest, Long bytes, List<DataType> types, Escape escape, boolean replace) {
        switch (this) {
            case DIFF:
                r.setRdbVisitor(new DiffRdbVisitor(r, conf, output, db, regexs, types));
                break;
            case COUNT:
                r.setRdbVisitor(new CountRdbVisitor(r, conf, output, db, regexs, types));
                break;
            case KEY:
                r.setRdbVisitor(new KeyRdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case JSON:
                r.setRdbVisitor(new JsonRdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case JSONLZSETEXPLODE:
                r.setRdbVisitor(new JsonlZSetExplodeRdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case JSONLHASHEXPLODE:
                r.setRdbVisitor(new JsonlHashExplodeRdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case JSONLKEYEDHASHEXPLODE:
                r.setRdbVisitor(new JsonlKeyedHashExplodeRdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case JSONL:
                r.setRdbVisitor(new JsonlRdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case JSONLBZ2:
                r.setRdbVisitor(new JsonlBZ2RdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case DUMP:
                r.setRdbVisitor(new DumpRdbVisitor(r, conf, output, db, regexs, types, replace));
                break;
            case RESP:
                r.setRdbVisitor(new RespRdbVisitor(r, conf, output, db, regexs, types, replace));
                break;
            case KEYVAL:
                r.setRdbVisitor(new KeyValRdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case MEM:
                r.setRdbVisitor(new MemRdbVisitor(r, conf, output, db, regexs, types, escape, largest, bytes));
                break;
            default:
                throw new AssertionError("Unsupported format '" + this + "'");
        }
    }
}
