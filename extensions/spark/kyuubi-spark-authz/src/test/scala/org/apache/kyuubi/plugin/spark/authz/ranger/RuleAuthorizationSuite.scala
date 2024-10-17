/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.plugin.spark.authz.ranger

// scalastyle:off
import org.mockito.ArgumentMatchers.{any, anyString, isA, eq => mockEq}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.kyuubi.plugin.spark.authz.SparkSessionProvider
import org.apache.kyuubi.service.authentication.ldap.DirSearchFactory
import org.apache.ranger.plugin.policyengine.RangerAccessRequest

import java.io.File
import java.nio.file.Files
import scala.reflect.io.Path.jfile2path

class RuleAuthorizationSuite extends AnyFunSuite
  with SparkSessionProvider with BeforeAndAfterAll {

  override protected val catalogImpl: String = "hive"

  private var tempDir: File = _

  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("kyuubi-test-").toFile
  }


  override def afterAll(): Unit = {
    if (tempDir != null) {
      tempDir.deleteRecursively() // 确保删除目录及其内容
    }
    spark.stop()
    super.afterAll()
  }

  // scalastyle:on
  test("KYUUBI #3605: test addAccessRequest") {
    val outputPath = tempDir.getAbsolutePath + "/small_files"
    spark.range(1, 100, 1, 100).write.parquet(outputPath)
    println("output path: " + outputPath)
    // requests: Seq[RangerAccessRequest],
    //      auditHandler: SparkRangerAuditHandler

    val plugin = mock[SparkRangerAdminPlugin.type]
    lenient
      .when(plugin.verify(Seq(any[RangerAccessRequest]), any[SparkRangerAuditHandler]))
      .thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")
    when(factory.getInstance(conf, "cn=user1,ou=PowerUsers,dc=mycorp,dc=com", "Blah"))
      .thenReturn(search)

    Thread.sleep(10000000L)
  }


}
