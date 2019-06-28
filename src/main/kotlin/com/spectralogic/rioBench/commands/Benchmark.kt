/*
 * **************************************************************************
 * Copyright 2014-2019 Spectra Logic Corporation. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 * this file except in compliance with the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file.
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * **************************************************************************
 */

package com.spectralogic.rioBench.commands

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.long
import com.spectralogic.escapepod.devintegration.FileToArchive
import com.spectralogic.escapepod.devintegration.FilesToArchive
import com.spectralogic.escapepod.devintegration.RioClient
import com.spectralogic.escapepod.devintegration.UserLoginCredentials
import com.spectralogic.escapepod.devintegration.createRioAuthClient
import com.spectralogic.escapepod.devintegration.createRioClient
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import java.net.URI
import java.time.Instant
import kotlin.math.pow
import kotlin.math.roundToLong

class Benchmark : CliktCommand(help = "Run the benchmark suite", name = "benchmark") {
    private val host: String by option("-h", "--hostname", help = "Rio hostname").required()
    private val broker: String by option("-b", "--broker", help = "Name of the broker to use").required()
    private val jobs: Long by option("-j", "--jobs", help = "number of jobs to launch").long().required()
    private val number: Long by option("-n", "--number", help = "number of items per job").long().required()
    private val size: Long by option("-s", "--size", help = "size of an individual item").long().required()
    private val units: Int by option("-u", "--units", help = "units for job item")
        .choice(Pair("b", 0), Pair("kb", 1), Pair("Mb", 2), Pair("Gb", 3), Pair("Tb", 4))
        .required()

    override fun run() {
        val itemSize: Long = size * (1024.0.pow(units)).roundToLong()
        val timedRioClient = TimedRioClient(host, UserLoginCredentials("spectra", "spectra"))
        runBlocking {
            LongRange(0, jobs)
                .asSequence()
                .map { job ->
                    LongRange(0, number)
                        .asSequence()
                        .map { file ->
                            FileToArchive(
                                "$job-$file-${Instant.now().toEpochMilli()}",
                                URI.create("aToZSequence://f"),
                                itemSize
                            )
                        }
                }
                .map { fileToArchive ->
                    GlobalScope.launch {
                        timedRioClient.acquireRioClient().archiveFiles(broker, FilesToArchive(fileToArchive.toList()))
                    }
                }
                .toList()
                .joinAll()
        }
        echo("Finished sending jobs")
    }

    inner class TimedRioClient(private val host: String, private val credentials: UserLoginCredentials) {
        private val mutex = Mutex()
        private var lastCreated: Instant = Instant.MIN
        private lateinit var rioClient: RioClient

        suspend fun acquireRioClient(): RioClient {
            try {
                mutex.lock()
                if (Instant.now().isAfter(lastCreated.plusSeconds(3000))) {
                    lastCreated = Instant.now()
                    rioClient = createRioClient(host, createRioAuthClient(host).createToken(credentials).token)
                }
            } finally {
                mutex.unlock()
            }
            return rioClient
        }
    }
}