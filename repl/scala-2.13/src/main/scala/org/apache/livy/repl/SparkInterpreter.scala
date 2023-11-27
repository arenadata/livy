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

package org.apache.livy.repl

import java.io.{File, PrintWriter}
import java.net.URLClassLoader
import java.nio.file.{Files, Paths}
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{CompletionCandidate, IMain, PresentationCompilationResult}
import scala.tools.nsc.interpreter.Results.Result
import org.apache.spark.SparkConf
import org.apache.spark.repl.SparkILoop

import scala.tools.nsc.interpreter.shell.{Completion, NoCompletion}

/**
 * This represents a Spark interpreter. It is not thread safe.
 */
class SparkInterpreter(protected override val conf: SparkConf) extends AbstractSparkInterpreter {

  private var sparkILoop: SparkILoop = _

  override def start(): Unit = {
    require(sparkILoop == null)

    val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
    outputDir.deleteOnExit()
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)

    val settings = new Settings()
    settings.processArguments(List("-Yrepl-class-based", "-Wconf:any:silent",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"), true)
    settings.usejavacp.value = true
    settings.embeddedDefaults(Thread.currentThread().getContextClassLoader())

    sparkILoop = new SparkILoop(null, new PrintWriter(outputStream, true))
    sparkILoop.createInterpreter(settings)
    sparkILoop.initializeCompiler()

    restoreContextClassLoader {
      sparkILoop.compilerClasspath
      sparkILoop.classLoader
      var classLoader = Thread.currentThread().getContextClassLoader
      while (classLoader != null) {
        if (classLoader.getClass.getCanonicalName ==
          "org.apache.spark.util.MutableURLClassLoader") {
          val extraJarPath = classLoader.asInstanceOf[URLClassLoader].getURLs()
            // Check if the file exists. Otherwise an exception will be thrown.
            .filter { u => u.getProtocol == "file" && new File(u.getPath).isFile }
            // Livy rsc and repl are also in the extra jars list. Filter them out.
            .filterNot { u => Paths.get(u.toURI).getFileName.toString.startsWith("livy-") }
            // Some bad spark packages depend on the wrong version of scala-reflect. Blacklist it.
            .filterNot { u =>
              Paths.get(u.toURI).getFileName.toString.contains("org.scala-lang_scala-reflect")
            }

          extraJarPath.foreach { p => debug(s"Adding $p to Scala interpreter's class path...") }
          sparkILoop.addUrlsToClassPath(extraJarPath: _*)
          classLoader = null
        } else {
          classLoader = classLoader.getParent
        }
      }

      postStart()
    }
  }

  override def close(): Unit = synchronized {
    super.close()

    if (sparkILoop != null) {
      sparkILoop.closeInterpreter()
      sparkILoop = null
    }
  }

  override protected def isStarted(): Boolean = {
    sparkILoop != null
  }

  override protected def interpret(code: String): Result = {
    sparkILoop.interpret(code)
  }

  override protected def completeCandidates(code: String, cursor: Int) : Array[CompletionCandidate] = {
    sparkILoop.intp.presentationCompile(cursor, code) match {
      case Right(c) => c.completionCandidates()._2.toArray
      case Left(_) => Array()
    }
  }

  override protected def valueOfTerm(name: String): Option[Any] = {
    // IMain#valueOfTerm will always return None, so use other way instead.
    Option(sparkILoop.valueOfTerm(name))
  }

  override protected def bind(name: String,
      tpe: String,
      value: Object,
      modifier: List[String]): Unit = {
    sparkILoop.beQuietDuring {
      sparkILoop.bind(name, tpe, value, modifier)
    }
  }
}