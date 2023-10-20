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
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonDSL._

import org.apache.livy.rsc.RSCConf

class ScalaInterpreterSpec extends BaseInterpreterSpec {

  implicit val formats = DefaultFormats

  override def createInterpreter(): Interpreter =
    new SparkInterpreter(new SparkConf()
    .setMaster("local[*]")
    .setAppName("test"))

  it should "execute `1 + 2` == 3" in withInterpreter { interpreter =>
    val response = interpreter.execute("1 + 2")
    response should equal (Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> addLineSeparator("val res0: Int = 3")
    ))
  }

  it should "execute multiple statements" in withInterpreter { interpreter =>
    var response = interpreter.execute("val x = 1")
    response should equal (Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> addLineSeparator("val x: Int = 1")
    ))

    response = interpreter.execute("val y = 2")
    response should equal (Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> addLineSeparator("val y: Int = 2")
    ))

    response = interpreter.execute("x + y")
    response should equal (Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> addLineSeparator("val res0: Int = 3")
    ))
  }

  it should "execute multiple statements in one block" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """
        |val x = 1
        |
        |val y = 2
        |
        |x + y
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> s"val x: Int = 1${System.lineSeparator()}val y: Int = 2${System.lineSeparator()}val res0: Int = 3${System.lineSeparator()}"
    ))
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

    response should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> addLineSeparator("val res0: Int = 3")
    ))
  }

  it should "capture stdout" in withInterpreter { interpreter =>
    val response = interpreter.execute("println(\"Hello World\")")
    response should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> addLineSeparator("Hello World")
    ))

    val resp1 = interpreter.execute(s"print(1)${System.lineSeparator()}print(2)")
    resp1 should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "12"
    ))

    val resp2 = interpreter.execute(s"println(1)${System.lineSeparator()}println(2)")
    resp2 should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> s"1${System.lineSeparator()}2${System.lineSeparator()}"
    ))
  }

  it should "report an error if accessing an unknown variable" in withInterpreter { interpreter =>
    interpreter.execute("x") match {
      case Interpreter.ExecuteError(ename, evalue, stacktrace) =>
        ename should equal ("Error")
        stacktrace.map(v => v.trim) should contain ("error: not found: value x")

      case other =>
        fail(s"Expected error, got $other.")
    }
  }

  it should "execute spark commands" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """sc.parallelize(0 to 1).collect""".stripMargin)

    response should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> addLineSeparator("val res0: Array[Int] = Array(0, 1)")
    ))
  }

  it should "handle statements ending with comments" in withInterpreter { interpreter =>
    // Test statements with only comments
    var response = interpreter.execute("""// comment""")
    response should equal(Interpreter.ExecuteSuccess(TEXT_PLAIN -> ""))

    response = interpreter.execute(
      """/*
        |comment
        |*/
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(TEXT_PLAIN -> ""))

    // Test statements ending with comments
    response = interpreter.execute(
      """val r = 1
        |// comment
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(TEXT_PLAIN -> addLineSeparator("val r: Int = 1")))

    response = interpreter.execute(
      """val r = 1
        |/*
        |comment
        |comment
        |*/
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(TEXT_PLAIN -> addLineSeparator("val r: Int = 1")))

    // Test statements ending with a mix of single line and multi-line comments
    response = interpreter.execute(
      """val r = 1
        |// comment
        |/*
        |comment
        |comment
        |*/
        |// comment
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(TEXT_PLAIN -> addLineSeparator("val r: Int = 1")))

    response = interpreter.execute(
      """val r = 1
        |/*
        |comment
        |// comment
        |comment
        |*/
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(TEXT_PLAIN -> addLineSeparator("val r: Int = 1")))

    // Make sure incomplete statement is still returned as incomplete statement.
    response = interpreter.execute("sc.")
    response should equal(Interpreter.ExecuteIncomplete())

    // Make sure incomplete statement is still returned as incomplete statement.
    response = interpreter.execute(
      """sc.
        |// comment
      """.stripMargin)
    response should equal(Interpreter.ExecuteIncomplete())

  }

  it should "return code completion candidates" in withInterpreter { interpreter =>
    val code =
      """"a".""".stripMargin
    val actual = interpreter.complete(code, code.length)
    actual should contain ("+")
    actual should contain ("charAt")
    actual should contain ("compareTo")
  }

}
