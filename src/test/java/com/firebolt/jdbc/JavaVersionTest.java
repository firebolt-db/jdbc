/*
 * Copyright 2023 Firebolt Analytics, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * NOTICE: THIS FILE HAS BEEN COPIED BY Firebolt Analytics, Inc. from the PostgreSQL Database Management System repository
 * URLs :
 *  - Repository: https://github.com/postgres/postgres/
 * Changes:
 *  - Replaced junit 4 with Jupiter
 */

/*
 *  PostgreSQL Database Management System
 *  (formerly known as Postgres, then as Postgres95)
 *  Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *  Portions Copyright (c) 1994, The Regents of the University of California
 *  Permission to use, copy, modify, and distribute this software and its
 *  documentation for any purpose, without fee, and without a written agreement
 *  is hereby granted, provided that the above copyright notice and this
 *  paragraph and the following two paragraphs appear in all copies.
 *
 *  IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 *  DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 *  LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 *  DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 *  THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 *  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 *  AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 *  ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 *  PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */
/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
package com.firebolt.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.postgresql.core.JavaVersion;

class JavaVersionTest {
	@Test
	void testGetRuntimeVersion() {
		String currentVersion = System.getProperty("java.version");
		String msg = "java.version = " + currentVersion + ", JavaVersion.getRuntimeVersion() = "
				+ JavaVersion.getRuntimeVersion();
		if (currentVersion.startsWith("1.8")) {
			assertEquals(org.postgresql.core.JavaVersion.v1_8, JavaVersion.getRuntimeVersion(), msg);
		}
	}
}