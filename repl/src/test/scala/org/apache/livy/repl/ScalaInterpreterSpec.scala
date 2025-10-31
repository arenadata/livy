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

import org.apache.spark.SparkConf
import org.json4s.{DefaultFormats, JObject, JValue}
import org.json4s.JsonDSL._

class ScalaInterpreterSpec extends BaseInterpreterSpec {

  implicit val formats = DefaultFormats

  override def createInterpreter(): Interpreter =
    new SparkInterpreter(new SparkConf())

  it should "execute `1 + 2` == 3" in withInterpreter { interpreter =>
    val response = interpreter.execute("1 + 2")
    response match {
      case Interpreter.ExecuteSuccess(obj: JObject) =>
        val s = (obj \ TEXT_PLAIN).extract[String]
        val last = s.linesIterator.filterNot(_.startsWith("warning:")).filter(_.trim.nonEmpty)
          .toList.lastOption.getOrElse("")
        last should include ("res0: Int = 3")
      case other =>
        fail(s"Expected ExecuteSuccess with JObject, got: $other")
    }
  }

  it should "execute multiple statements" in withInterpreter { interpreter =>
    val r1 = interpreter.execute("val x = 1")
    r1 match {
      case Interpreter.ExecuteSuccess(obj: JObject) =>
        val s = (obj \ TEXT_PLAIN).extract[String]
        s should include ("x: Int = 1")
      case other => fail(s"Expected success, got: $other")
    }

    val r2 = interpreter.execute("val y = 2")
    r2 match {
      case Interpreter.ExecuteSuccess(obj: JObject) =>
        val s = (obj \ TEXT_PLAIN).extract[String]
        s should include ("y: Int = 2")
      case other => fail(s"Expected success, got: $other")
    }

    val r3 = interpreter.execute("x + y")
    r3 match {
      case Interpreter.ExecuteSuccess(obj: JObject) =>
        val s = (obj \ TEXT_PLAIN).extract[String]
        val last = s.linesIterator.filterNot(_.startsWith("warning:")).filter(_.trim.nonEmpty)
          .toList.lastOption.getOrElse("")
        last should include ("res0: Int = 3")
      case other => fail(s"Expected success, got: $other")
    }
  }

  it should "execute multiple statements in one block" in withInterpreter { interpreter =>
    val response = interpreter.execute("val x = 1; val y = 2; x + y")
    response match {
      case Interpreter.ExecuteSuccess(obj: JObject) =>
        val s = (obj \ TEXT_PLAIN).extract[String]
        val lines = s.linesIterator.filterNot(_.startsWith("warning:"))
          .filter(_.trim.nonEmpty).toList
        val merged = lines.mkString("\n")
        merged should include ("x: Int = 1")
        merged should include ("y: Int = 2")
        lines.last should include ("= 3")
      case other =>
        fail(s"Expected ExecuteSuccess with JObject, got: $other")
    }
  }

  it should "do table magic" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """val x = List(List(1, "a"), List(3, "b"))
        |%table x
      """.stripMargin)

    response should equal(Interpreter.ExecuteSuccess(
      APPLICATION_LIVY_TABLE_JSON -> (
        ("headers" -> List(
          ("type" -> "BIGINT_TYPE") ~ ("name" -> "0"),
          ("type" -> "STRING_TYPE") ~ ("name" -> "1")
        )) ~
          ("data" -> List(
            List[JValue](1, "a"),
            List[JValue](3, "b")
          ))
        )
    ))
  }

  it should "allow magic inside statements" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """val x = List(List(1, "a"), List(3, "b"))
        |%table x
        |1 + 2
      """.stripMargin)
    response match {
      case Interpreter.ExecuteSuccess(obj: JObject) =>
        val s = (obj \ TEXT_PLAIN).extract[String]
        val last = s.linesIterator.filterNot(_.startsWith("warning:")).filter(_.trim.nonEmpty)
          .toList.lastOption.getOrElse("")
        last should include ("res0: Int = 3")
      case other =>
        fail(s"Expected ExecuteSuccess with JObject, got: $other")
    }
  }

  it should "capture stdout" in withInterpreter { interpreter =>
    val response = interpreter.execute("""println("Hello World")""")
    response match {
      case Interpreter.ExecuteSuccess(obj: JObject) =>
        (obj \ TEXT_PLAIN).extract[String] should equal ("Hello World\n")
      case other => fail(s"Expected success, got: $other")
    }

    val resp1 = interpreter.execute("print(1)\nprint(2)")
    resp1 match {
      case Interpreter.ExecuteSuccess(obj: JObject) =>
        (obj \ TEXT_PLAIN).extract[String] should equal ("12")
      case other => fail(s"Expected success, got: $other")
    }

    val resp2 = interpreter.execute("println(1)\nprintln(2)")
    resp2 match {
      case Interpreter.ExecuteSuccess(obj: JObject) =>
        (obj \ TEXT_PLAIN).extract[String] should equal ("1\n2\n")
      case other => fail(s"Expected success, got: $other")
    }
  }

  it should "report an error if accessing an unknown variable" in withInterpreter { interpreter =>
    interpreter.execute("x") match {
      case Interpreter.ExecuteError(ename, evalue, traceback) =>
        ename should equal ("Error")
        val combined = (evalue :: traceback.toList).mkString("\n")
        combined should include ("not found: value x")
      case other =>
        fail(s"Expected error, got $other.")
    }
  }

  it should "execute spark commands" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """sc.parallelize(0 to 1).map { i => i+1 }.collect""".stripMargin)
    response match {
      case Interpreter.ExecuteSuccess(obj: JObject) =>
        val s = (obj \ TEXT_PLAIN).extract[String]
        val last = s.linesIterator.filterNot(_.startsWith("warning:")).filter(_.trim.nonEmpty)
          .toList.lastOption.getOrElse("")
        last should include ("res0: Array[Int] = Array(1, 2)")
      case other => fail(s"Expected success, got: $other")
    }
  }

  it should "return code completion candidates" in withInterpreter { interpreter =>
    val code = """"a".""".stripMargin
    val actual = interpreter.complete(code, code.length)
    actual should contain ("+")
    actual should contain ("charAt")
    actual should contain ("compareTo")
  }
}
