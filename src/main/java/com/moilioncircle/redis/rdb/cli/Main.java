/*
 * Copyright 2016-2017 Leon Chen
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

package com.moilioncircle.redis.rdb.cli;

import com.moilioncircle.redis.rdb.cli.cmd.RctCommand;
import com.moilioncircle.redis.rdb.cli.cmd.RdtCommand;
import com.moilioncircle.redis.rdb.cli.cmd.RetCommand;
import com.moilioncircle.redis.rdb.cli.cmd.RmtCommand;
import com.moilioncircle.redis.rdb.cli.cmd.RstCommand;

/**
 * @author Baoyi Chen
 */
public class Main {
	public static void main(String[] args) throws Exception {
		String[] args0 = new String[args.length - 1];
		System.arraycopy(args, 1, args0, 0, args0.length);
		switch (args[0]) {
			case "rct":
				RctCommand.run(args0);
				break;
			case "rdt":
				RdtCommand.run(args0);
				break;
			case "ret":
				RetCommand.run(args0);
				break;
			case "rmt":
				RmtCommand.run(args0);
				break;
			case "rst":
				RstCommand.run(args0);
				break;
			default:
				throw new UnsupportedOperationException(args[0]);
		}
	}
}
