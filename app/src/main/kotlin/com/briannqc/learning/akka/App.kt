package com.briannqc.learning.akka

import akka.actor.*
import akka.event.Logging
import akka.pattern.Patterns.ask
import scala.concurrent.ExecutionContext
import java.util.concurrent.CompletableFuture


class App {
    val greeting: String
        get() {
            return "Hello World!"
        }
}

class FirstActor : AbstractActor() {
    companion object {
        fun props(): Props {
            return Props.create(FirstActor::class.java)
        }
    }

    private val log = Logging.getLogger(context.system, this)

    override fun preStart() {
        log.info("FirstActor started")
    }

    override fun postStop() {
        log.info("FirstActor stopped")
    }

    override fun createReceive(): Receive {
        return receiveBuilder()
            .matchEquals("printit") { println("I'm $self, telling to $sender") }
            .match(String::class.java) { msg -> println("Got $msg from $sender") }
            .build()
    }
}

class PrinterActor : AbstractActor() {

    companion object {
        fun props(): Props {
            return Props.create(PrinterActor::class.java)
        }
    }

    class PrintFinalResult(val total: Int)

    private val log = Logging.getLogger(context.system, this)

    override fun preStart() {
        log.info("$self started")
    }

    override fun postStop() {
        log.info("$self stopped")
    }

    override fun createReceive(): Receive {
        return receiveBuilder()
            .match(PrintFinalResult::class.java) { result: PrintFinalResult ->
                log.info("Received PrintFinalResult message from $sender")
                log.info("The text has a total number of ${result.total} words")
            }
            .build()
    }
}

class ReadingActor(
    private val text: String
) : AbstractActor() {

    companion object {
        fun props(text: String): Props {
            return Props.create(ReadingActor::class.java, text)
        }
    }

    class ReadLines

    private val log = Logging.getLogger(context.system, this)

    override fun preStart() {
        log.info("$self started")
    }

    override fun postStop() {
        log.info("$self stopped")
    }


    override fun createReceive(): Receive {
        return receiveBuilder()
            .match(ReadLines::class.java) {
                log.info("Received ReadLines message from $sender")

                val lines = text.split("\n")
                val futures = mutableListOf<CompletableFuture<Int>>()
                for ((i, line) in lines.withIndex()) {
                    val wordCounterActorRef = context.actorOf(
                        Props.create(WordCounterActor::class.java),
                        "word-counter-$i"
                    )

                    val future = CompletableFuture<Int>()

                    ask(wordCounterActorRef, WordCounterActor.CountWords(line), 1000L)
                        .onComplete({ result ->
                            when (result) {
                                is scala.util.Success -> future.complete(result.value() as Int?)
                                is scala.util.Failure -> future.completeExceptionally(result.exception())
                                else -> {}
                            }
                        }, ExecutionContext.global())
                    futures.add(future)
                }

                val totalNumberOfWords = futures.sumOf { f -> f.join() }
                val printerActorRef = context.actorOf(PrinterActor.props(), "Printer-Actor")
                printerActorRef.forward(PrinterActor.PrintFinalResult(totalNumberOfWords), context)
            }
            .build()
    }
}

class WordCounterActor : AbstractActor() {

    data class CountWords(val line: String)

    private val log = Logging.getLogger(context.system, this)

    override fun preStart() {
        log.info("$self started")
    }

    override fun postStop() {
        log.info("$self stopped")
    }

    override fun createReceive(): Receive {
        return receiveBuilder()
            .match(CountWords::class.java) { msg ->
                log.info("Received CountWords message: '$msg' from $sender")
                handleCountWordsMessage(msg)
            }
            .build()
    }

    private fun handleCountWordsMessage(msg: CountWords) {
        try {
            val numberOfWords = countWordsFromLine(msg.line)
            sender.tell(numberOfWords, self)
        } catch (ex: Exception) {
            sender.tell(Status.Failure(ex), self)
            throw ex
        }
    }

    private fun countWordsFromLine(line: String): Int {
        var numberOfWords = 0
        for (possibleWord in line.split(" ")) {
            if (possibleWord.trim().isNotEmpty()) {
                numberOfWords++
            }
        }
        return numberOfWords
    }
}

fun main() {
    val actorSystem = ActorSystem.create("test-system")

    val text = """
        Ask ChatGPT: What is Akka?
        
        Answer:
        Akka is a free and open-source toolkit and runtime for building highly concurrent, distributed, and resilient
        message-driven applications on the JVM (Java Virtual Machine). It is written in Scala and provides an
        actor-based model for concurrency and distribution.

        In Akka, actors are the basic building blocks for building distributed and concurrent applications. An actor is
        a self-contained and isolated unit of computation that can communicate with other actors by exchanging messages.
        Actors encapsulate state and behavior, and they are scheduled and executed by the Akka runtime.

        Akka provides a rich set of abstractions and tools for building scalable, resilient, and high-performance
        applications, such as actors, actor systems, clustering, remoting, persistence, streams, and more. It also
        provides integration with other JVM libraries and technologies, such as Java, Scala, and Apache Cassandra,
        to name a few.

        Akka is widely used for building distributed systems, microservices, event-driven applications, and more.
        It is designed to be highly flexible and modular, making it easy to customize and extend according to specific
        needs and requirements.
    """.trimIndent()

    val readingActor = actorSystem.actorOf(ReadingActor.props(text), "ReadingActor")


    readingActor.tell(ReadingActor.ReadLines(), ActorRef.noSender())
}
