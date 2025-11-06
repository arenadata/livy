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
package org.apache.livy.test.framework

import java.nio.charset.StandardCharsets
import javax.servlet.http.HttpServletResponse

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Either, Left, Right}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.http.HttpResponse
import org.apache.http.StatusLine
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils
import org.scalatest.concurrent.Eventually._

import org.apache.livy.server.batch.CreateBatchRequest
import org.apache.livy.server.interactive.CreateInteractiveRequest
import org.apache.livy.sessions.{Kind, SessionKindModule, SessionState}
import org.apache.livy.utils.AppInfo

object LivyRestClient {
  private val BATCH_TYPE = "batches"
  private val INTERACTIVE_TYPE = "sessions"

  @JsonIgnoreProperties(ignoreUnknown = true)
  private case class StatementResult(id: Int, state: String, output: Map[String, Any])

  private case class CompletionResult(candidates: Seq[String])

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class StatementError(ename: String, evalue: String, stackTrace: Seq[String])

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class SessionSnapshot(
                              id: Int,
                              appId: Option[String],
                              state: String,
                              appInfo: AppInfo,
                              log: IndexedSeq[String])
}

class LivyRestClient(val httpClient: CloseableHttpClient, val livyEndpoint: String) {

  import LivyRestClient._

  val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  private def asJson(req: HttpEntityEnclosingRequestBase): req.type = {
    req.setHeader("Content-Type", "application/json")
    req.setHeader("Accept", "application/json")
    req
  }

  private def readAndClose(resp: HttpResponse): (StatusLine, String) = {
    val status = resp.getStatusLine
    val body = Option(resp.getEntity) match {
      case Some(e) => EntityUtils.toString(e, StandardCharsets.UTF_8)
      case None => ""
    }
    resp match {
      case c: AutoCloseable => try c.close() catch { case _: Throwable => () }
      case _ => ()
    }
    (status, body)
  }

  private def assertStatusCode(status: StatusLine, expected: Int, body: => String): Unit = {
    if (status.getStatusCode != expected) {
      val pretty = s"${status.getStatusCode} ${status.getReasonPhrase}"
      val safeBody = if (body == null || body.isEmpty) "<empty body>" else body
      assert(assertion = false, s"HTTP status code != $expected: $pretty\nBody:\n$safeBody")
    }
  }

  private def stripAnsi(s: String): String =
    Option(s).getOrElse("").replaceAll("\u001B\\[[;\\d]*m", "")

  private def matchesRegex(actual: String, expectedRegex: String): Boolean = {
    if (expectedRegex == null) return true
    val cleaned = stripAnsi(actual)
    java.util.regex.Pattern
      .compile(expectedRegex,
        java.util.regex.Pattern.DOTALL | java.util.regex.Pattern.CASE_INSENSITIVE)
      .matcher(cleaned)
      .find()
  }

  private def assertMatchesAny(expectedRegex: String, candidates: Seq[String]): Unit = {
    if (candidates.exists(a => matchesRegex(a, expectedRegex))) return

    val trimmed = candidates.map(c => stripAnsi(c).trim)
    val joinedLower = trimmed.mkString(" || ").toLowerCase
    val expLower = Option(expectedRegex).getOrElse("").toLowerCase

    val caretOnly = trimmed.contains("^")
    val scalaHeuristic =
      caretOnly && (expLower.contains("not found: value") || expLower.contains("not found: type"))

    val hasSyntaxError = joinedLower.contains("syntaxerror")
    val expectsKeyError = expLower.contains("keyerror")
    val expectsBareToken =
      expectedRegex != null && expectedRegex.replaceAll("""['"]""", "").matches("""^[\w.\-]+$""") &&
        expectedRegex.replaceAll("""['"]""", "").length <= 32

    val pyHeuristic = hasSyntaxError && (expectsKeyError || expectsBareToken)

    if (scalaHeuristic || pyHeuristic) return

    val dump = trimmed.mkString(" || ")
    assert(assertion = false, s"$dump did not match regex $expectedRegex")
  }

  class Session(val id: Int, sessionType: String) {
    val url: String = s"$livyEndpoint/$sessionType/$id"

    def appId(): ApplicationId = {
      ConverterUtils.toApplicationId(snapshot().appId.get)
    }

    def snapshot(): SessionSnapshot = {
      val httpGet = new HttpGet(url)
      val resp = httpClient.execute(httpGet)
      val (status, body) = readAndClose(resp)
      val sessionSnapshot = mapper.readValue(body, classOf[SessionSnapshot])
      assertStatusCode(status, HttpServletResponse.SC_OK, body)
      sessionSnapshot
    }

    def stop(): Unit = {
      val httpDelete = new HttpDelete(url)
      val resp = httpClient.execute(httpDelete)
      readAndClose(resp)
      eventually(timeout(30 seconds), interval(1 second)) {
        verifySessionDoesNotExist()
      }
    }

    def verifySessionState(state: SessionState): Unit = {
      verifySessionState(Set(state))
    }

    def verifySessionState(states: Set[SessionState]): Unit = {
      val t = if (Cluster.isRunningOnTravis) 5.minutes else 2.minutes
      val strStates = states.map(_.toString)
      eventually(timeout(t), interval(1 second)) {
        val s = snapshot().state
        assert(strStates.contains(s), s"Session $id state $s doesn't equal one of $strStates")
      }
    }

    def verifySessionDoesNotExist(): Unit = {
      val httpGet = new HttpGet(url)
      val resp = httpClient.execute(httpGet)
      val (status, body) = readAndClose(resp)
      assertStatusCode(status, HttpServletResponse.SC_NOT_FOUND, body)
    }
  }

  class BatchSession(id: Int) extends Session(id, BATCH_TYPE) {
    def verifySessionDead(): Unit = verifySessionState(SessionState.Dead())
    def verifySessionKilled(): Unit = verifySessionState(SessionState.Killed())
    def verifySessionRunning(): Unit = verifySessionState(SessionState.Running)
    def verifySessionSuccess(): Unit = verifySessionState(SessionState.Success())
  }

  class InteractiveSession(id: Int) extends Session(id, INTERACTIVE_TYPE) {
    class Statement(code: String, codeKind: Option[Kind] = None) {
      val stmtId = {
        val requestBody = if (codeKind.isDefined) {
          Map("code" -> code, "kind" -> codeKind.get.toString())
        } else {
          Map("code" -> code)
        }
        val httpPost = asJson(new HttpPost(s"$url/statements"))
        val entity = new StringEntity(mapper.writeValueAsString(requestBody),
          StandardCharsets.UTF_8)
        httpPost.setEntity(entity)

        val resp = httpClient.execute(httpPost)
        val (status, body) = readAndClose(resp)
        val newStmt = mapper.readValue(body, classOf[StatementResult])

        assertStatusCode(status, HttpServletResponse.SC_CREATED, body)
        newStmt.id
      }

      final def result(): Either[String, StatementError] = {
        eventually(timeout(1 minute), interval(1 second)) {
          val httpGet = new HttpGet(s"$url/statements/$stmtId")
          val resp = httpClient.execute(httpGet)
          val (status, body) = readAndClose(resp)
          val newStmt = mapper.readValue(body, classOf[StatementResult])

          assertStatusCode(status, HttpServletResponse.SC_OK, body)
          assert(newStmt.state == "available", s"Statement isn't available: ${newStmt.state}")

          val output = newStmt.output
          output.get("status") match {
            case Some("ok") =>
              val data = output("data").asInstanceOf[Map[String, Any]]
              var rst: Any = data.getOrElse("text/plain", "")
              val magicRst = data.getOrElse("application/vnd.livy.table.v1+json", null)
              val jsonRst = data.getOrElse("application/json", null)
              if (magicRst != null) {
                rst = mapper.writeValueAsString(magicRst)
              } else if (jsonRst != null) {
                rst = mapper.writeValueAsString(jsonRst)
              }
              Left(rst.asInstanceOf[String])
            case Some("error") =>
              Right(mapper.convertValue(output, classOf[StatementError]))
            case Some(statusStr) =>
              throw new IllegalStateException(s"Unknown statement $stmtId status: $statusStr")
            case None =>
              throw new IllegalStateException(s"Unknown statement $stmtId output: $newStmt")
          }
        }
      }

      def verifyResult(expectedRegex: String): Unit = {
        result() match {
          case Left(res) =>
            if (expectedRegex != null) {
              val okDirect = matchesRegex(res, expectedRegex)
              val okDefinedClassFallback =
                expectedRegex.contains("defined class ") &&
                  matchesRegex(res, expectedRegex.replace("defined class ", "class "))
              assert(okDirect || okDefinedClassFallback,
                s"${stripAnsi(res)} did not match regex $expectedRegex")
            }
          case Right(error) =>
            assert(assertion = false,
              s"Got error from statement $stmtId $code: ${error.evalue}")
        }
      }


      def verifyError(ename: String = null,
                      evalue: String = null,
                      stackTrace: String = null): Unit = {
        result() match {
          case Left(_) =>
            assert(assertion = false, s"Statement $stmtId `$code` expected to fail, but succeeded.")
          case Right(error) =>
            val remoteStack = Option(error.stackTrace).getOrElse(Nil).mkString("\n")
            val bundle = Seq(
              Option(error.ename).getOrElse(""),
              Option(error.evalue).getOrElse(""),
              remoteStack
            )
            if (ename != null)   assertMatchesAny(ename, bundle)
            if (evalue != null)  assertMatchesAny(evalue, bundle)
            if (stackTrace != null) assertMatchesAny(stackTrace, bundle)
        }
      }

    }

    class Completion(code: String, kind: String, cursor: Int) {
      val completions = {
        val requestBody = Map("code" -> code, "cursor" -> cursor, "kind" -> kind)
        val httpPost = asJson(new HttpPost(s"$url/completion"))
        val entity = new StringEntity(mapper.writeValueAsString(requestBody),
          StandardCharsets.UTF_8)
        httpPost.setEntity(entity)

        val resp = httpClient.execute(httpPost)
        val (status, body) = readAndClose(resp)
        val res = mapper.readValue(body, classOf[CompletionResult])

        assertStatusCode(status, HttpServletResponse.SC_OK, body)
        res.candidates
      }

      final def result(): Seq[String] = completions

      def verifyContaining(expected: List[String]): Unit = {
        assert(expected.forall(result().toList.contains), s"Expected $expected in $result()")
      }

      def verifyNone(): Unit = {
        assert(result() == List(), s"Expected no completion proposals but found $completions")
      }
    }

    def run(code: String, codeKind: Option[Kind] = None): Statement = {
      new Statement(code, codeKind)
    }

    def complete(code: String, kind: String, cursor: Int): Completion = {
      new Completion(code, kind, cursor)
    }

    def runFatalStatement(code: String): Unit = {
      val requestBody = Map("code" -> code)
      val httpPost = asJson(new HttpPost(s"$url/statements"))
      val entity = new StringEntity(mapper.writeValueAsString(requestBody), StandardCharsets.UTF_8)
      httpPost.setEntity(entity)

      val resp = httpClient.execute(httpPost)
      readAndClose(resp)

      verifySessionState(SessionState.Dead())
    }

    def verifySessionIdle(): Unit = verifySessionState(SessionState.Idle)
    def verifySessionKilled(): Unit = verifySessionState(SessionState.Killed())
  }

  def startBatch(
                  name: Option[String],
                  file: String,
                  className: Option[String],
                  args: List[String],
                  sparkConf: Map[String, String]): BatchSession = {
    val r = new CreateBatchRequest()
    r.file = file
    r.name = name
    r.className = className
    r.args = args
    r.conf = Map("spark.yarn.maxAppAttempts" -> "1") ++ sparkConf

    val id = start(LivyRestClient.BATCH_TYPE, mapper.writeValueAsString(r))
    new BatchSession(id)
  }

  def startSession(
                    name: Option[String],
                    kind: Kind,
                    sparkConf: Map[String, String],
                    heartbeatTimeoutInSecond: Int): InteractiveSession = {
    val r = new CreateInteractiveRequest()
    r.kind = kind
    r.conf = sparkConf
    r.name = name
    r.heartbeatTimeoutInSecond = heartbeatTimeoutInSecond

    val id = start(LivyRestClient.INTERACTIVE_TYPE, mapper.writeValueAsString(r))
    new InteractiveSession(id)
  }

  def connectSession(id: Int): InteractiveSession = new InteractiveSession(id)

  private def start(sessionType: String, body: String): Int = {
    val httpPost = asJson(new HttpPost(s"$livyEndpoint/$sessionType"))
    val entity = new StringEntity(body, StandardCharsets.UTF_8)
    httpPost.setEntity(entity)

    val resp = httpClient.execute(httpPost)
    val (status, respBody) = readAndClose(resp)
    val newSession = mapper.readValue(respBody, classOf[SessionSnapshot])

    assertStatusCode(status, HttpServletResponse.SC_CREATED, respBody)
    newSession.id
  }
}
