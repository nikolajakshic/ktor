/*
 * Copyright 2014-2019 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.client.engine.apache

import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.utils.io.*
import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import org.apache.http.*
import org.apache.http.nio.*
import org.apache.http.nio.protocol.*
import org.apache.http.protocol.*
import java.net.*
import java.nio.*
import kotlin.coroutines.*

internal class ApacheResponseConsumer(
    override val coroutineContext: CoroutineContext,
    private val requestData: HttpRequestData?,
    private val block: (HttpResponse, ByteReadChannel) -> Unit
) : HttpAsyncResponseConsumer<Unit>, CoroutineScope {
    private val interestController = InterestControllerHolder()

    private val channel = ByteChannel()
    private val contentChannel: ByteReadChannel = channel

    override fun consumeContent(decoder: ContentDecoder, ioctrl: IOControl) {
        var result = 0
        channel.writeAvailable {
            result = decoder.read(it)
        }
        // Reader should call resume after read

        if (result < 0 || decoder.isCompleted) {
            channel.close()
            return
        }

        if (result == 0) {
            interestController.suspendInput(ioctrl)
        }
    }

    override fun failed(cause: Exception) {
        val mappedCause = when {
            cause is ConnectException && cause.isTimeoutException() -> ConnectTimeoutException(requestData!!, cause)
            cause is java.net.SocketTimeoutException -> SocketTimeoutException(requestData!!, cause)
            else -> cause
        }

        channel.cancel(CancellationException("Failed to execute request", mappedCause))
    }

    override fun cancel(): Boolean {
        channel.cancel()
        return true
    }

    override fun close() {
    }

    override fun getException(): Exception? = channel.closedCause as? Exception

    override fun getResult() {
    }

    override fun isDone(): Boolean = channel.isClosedForWrite

    override fun responseCompleted(context: HttpContext) {
    }

    override fun responseReceived(response: HttpResponse) {
        block(response, contentChannel)
    }
}
