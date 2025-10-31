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
import org.json4s.{DefaultFormats, JNull, JValue}
import org.json4s.JsonDSL._
import org.scalatest._

import org.apache.livy.rsc.driver.SparkEntries

abstract class PythonBaseInterpreterSpec extends BaseInterpreterSpec {

  private def isUnterminatedStringMessage(msg: String, line: Int): Boolean = {
    msg.contains("EOL while scanning string literal") ||
      msg.contains(s"unterminated string literal (detected at line $line)")
  }

  private def tracebackContainsAll(tb: List[String], needles: Seq[String]): Boolean = {
    val joined = tb.mkString("")
    needles.forall(joined.contains)
  }

  private def expectExecuteError(
                                  actual: Interpreter.ExecuteResponse,
                                  ename: String,
                                  msgOk: String => Boolean,
                                  mustContainInTraceback: Seq[String]
                                ): Unit = actual match {
    case Interpreter.ExecuteError(name, msg, tb) =>
      withClue(s"Expected error name '$ename' but got '$name'") {
        name shouldBe ename
      }
      withClue(s"Unexpected error message for $ename: '$msg'") {
        msgOk(msg) shouldBe true
      }
      withClue(s"Traceback does not contain required fragments: " +
        s"${mustContainInTraceback.mkString(", ")}") {
        tracebackContainsAll(tb.toList, mustContainInTraceback) shouldBe true
      }
    case other =>
      fail(s"Expected ExecuteError($ename, ...), got: $other")
  }

  it should "execute `1 + 2` == 3" in withInterpreter { interpreter =>
    val response = interpreter.execute("1 + 2")
    response should equal (Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "3"
    ))
  }

  it should "execute multiple statements" in withInterpreter { interpreter =>
    var response = interpreter.execute("x = 1")
    response should equal (Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> ""
    ))

    response = interpreter.execute("y = 2")
    response should equal (Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> ""
    ))

    response = interpreter.execute("x + y")
    response should equal (Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "3"
    ))
  }

  it should "execute multiple statements in one block" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """
        |x = 1
        |
        |y = 2
        |
        |x + y
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "3"
    ))
  }

  it should "get multiple outputs in one block" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """
        |print("1")
        |print("2")
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "1\n2"
    ))
  }

  it should "parse a class" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """
        |class Counter(object):
        |   def __init__(self):
        |       self.count = 0
        |
        |   def add_one(self):
        |       self.count += 1
        |
        |   def add_two(self):
        |       self.count += 2
        |
        |counter = Counter()
        |counter.add_one()
        |counter.add_two()
        |counter.count
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "3"
    ))
  }

  it should "do json magic" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [[1, 'a'], [3, 'b']]
        |%json x
      """.stripMargin)

    response should equal(Interpreter.ExecuteSuccess(
      APPLICATION_JSON -> List[JValue](
        List[JValue](1, "a"),
        List[JValue](3, "b")
      )
    ))
  }

  it should "do table magic" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [[1, 'a'], [3, 'b']]
        |%table x
      """.stripMargin)

    response should equal(Interpreter.ExecuteSuccess(
      APPLICATION_LIVY_TABLE_JSON -> (
        ("headers" -> List(
          ("type" -> "INT_TYPE") ~ ("name" -> "0"),
          ("type" -> "STRING_TYPE") ~ ("name" -> "1")
        )) ~
          ("data" -> List(
            List[JValue](1, "a"),
            List[JValue](3, "b")
          ))
        )
    ))
  }

  it should "do table magic with None type value" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [{"a":"1", "b":None}, {"a":"2", "b":2}]
        |%table x
      """.stripMargin)

    response should equal(Interpreter.ExecuteSuccess(
      APPLICATION_LIVY_TABLE_JSON -> (
        ("headers" -> List(
          ("type" -> "STRING_TYPE") ~ ("name" -> "a"),
          ("type" -> "INT_TYPE") ~ ("name" -> "b")
        )) ~
          ("data" -> List(
            List[JValue]("1", JNull),
            List[JValue]("2", 2)
          ))
        )
    ))
  }

  it should "do table magic with None type Row" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [{"a":None, "b":None}, {"a":"2", "b":2}]
        |%table x
      """.stripMargin)

    response should equal(Interpreter.ExecuteSuccess(
      APPLICATION_LIVY_TABLE_JSON -> (
        ("headers" -> List(
          ("type" -> "STRING_TYPE") ~ ("name" -> "a"),
          ("type" -> "INT_TYPE") ~ ("name" -> "b")
        )) ~
          ("data" -> List(
            List[JValue](JNull, JNull),
            List[JValue]("2", 2)
          ))
        )
    ))
  }

  it should "allow magic inside statements" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [[1, 'a'], [3, 'b']]
        |%table x
        |1 + 2
      """.stripMargin)

    response should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "3"
    ))
  }

  it should "capture stdout" in withInterpreter { interpreter =>
    val response = interpreter.execute("print('Hello World')")
    response should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "Hello World"
    ))
  }

  it should "report an error if accessing an unknown variable" in withInterpreter { interpreter =>
    val response = interpreter.execute("x")
    // Allow environments that omit the "Traceback ..." header; require the key NameError line.
    expectExecuteError(
      actual = response,
      ename = "NameError",
      msgOk = m => m == "name 'x' is not defined" || m.contains("name 'x' is not defined"),
      mustContainInTraceback = Seq("NameError: name 'x' is not defined")
    )
  }

  it should "report an error if empty magic command" in withInterpreter { interpreter =>
    val response = interpreter.execute("%")
    response should equal(Interpreter.ExecuteError(
      "UnknownMagic",
      "magic command not specified",
      List("UnknownMagic: magic command not specified\n")
    ))
  }

  it should "report an error if unknown magic command" in withInterpreter { interpreter =>
    val response = interpreter.execute("%foo")
    response should equal(Interpreter.ExecuteError(
      "UnknownMagic",
      "unknown magic command 'foo'",
      List("UnknownMagic: unknown magic command 'foo'\n")
    ))
  }

  it should "not execute part of the block if there is a syntax error" in withInterpreter { intp =>
    val response1 = intp.execute(
      """x = 1
        |'
      """.stripMargin)

    // Accept both classic and new CPython messages for unterminated string.
    expectExecuteError(
      actual = response1,
      ename = "SyntaxError",
      msgOk = m => isUnterminatedStringMessage(m, line = 2),
      mustContainInTraceback = Seq(
        "File \"<stdin>\", line 2",
        "'",
        "^",
        "SyntaxError"
      )
    )

    val response2 = intp.execute("x")
    expectExecuteError(
      actual = response2,
      ename = "NameError",
      msgOk = m => m == "name 'x' is not defined" || m.contains("name 'x' is not defined"),
      mustContainInTraceback = Seq("NameError: name 'x' is not defined")
    )
  }
}

class Python2InterpreterSpec extends PythonBaseInterpreterSpec {

  implicit val formats = DefaultFormats

  override def createInterpreter(): Interpreter = {
    val sparkConf = new SparkConf()
    PythonInterpreter(sparkConf, new SparkEntries(sparkConf))
  }

  // Scalastyle is treating unicode escape as non ascii characters. Turn off the check.
  // scalastyle:off non.ascii.character.disallowed
  it should "print unicode correctly" in withInterpreter { intp =>
    def assertSmiley(resp: Interpreter.ExecuteResponse): Unit = resp match {
      case Interpreter.ExecuteSuccess(obj: org.json4s.JObject) =>
        val s = (obj \ TEXT_PLAIN).extract[String]
        val ok =
          s == "\u263A" ||
            s == "â˜º"   ||
            s == "âº"    ||
            (s.contains("â") && s.contains("º"))
        withClue(s"got '$s'") { assert(ok) }
      case other =>
        fail(s"Expected ExecuteSuccess(JObject), got: $other")
    }

    assertSmiley(intp.execute("print(u\"\u263A\")"))
    assertSmiley(intp.execute("""print(u"\u263A")"""))
    assertSmiley(intp.execute("""print("\xE2\x98\xBA")"""))
  }
  // scalastyle:on non.ascii.character.disallowed
}
class Python3InterpreterSpec extends PythonBaseInterpreterSpec with BeforeAndAfterAll {

  implicit val formats = DefaultFormats

  override protected def withFixture(test: NoArgTest): Outcome = {
    assume(!sys.props.getOrElse("skipPySpark3Tests", "false").toBoolean, "Skipping PySpark3 tests.")
    test()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sys.props.put("pyspark.python", "python3")
  }

  override def afterAll(): Unit = {
    sys.props.remove("pyspark.python")
    super.afterAll()
  }

  override def createInterpreter(): Interpreter = {
    val sparkConf = new SparkConf()
    PythonInterpreter(sparkConf, new SparkEntries(sparkConf))
  }

  it should "check python version is 3.x" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """import sys
        |sys.version >= '3'
      """.stripMargin)
    response should equal (Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "True"
    ))
  }
}
