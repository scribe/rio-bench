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
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.long
import com.spectralogic.escapepod.devintegration.FileToArchive
import com.spectralogic.escapepod.devintegration.FilesToArchive
import com.spectralogic.escapepod.devintegration.RioJob
import com.spectralogic.escapepod.devintegration.UserLoginCredentials
import com.spectralogic.escapepod.devintegration.createRioAuthClient
import com.spectralogic.escapepod.devintegration.createRioClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
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
    private val threads: Int by option("-t", "--threads").int().default(4)

    override fun run() {
        runBlocking {
            for (job in sendJobs(buildJobs(jobNumber()))) {
                echo(job.id)
            }
        }
    }

    fun CoroutineScope.jobNumber(): ReceiveChannel<Long> = produce {
        LongRange(1, jobs).forEach {
            send(it)
        }
    }

    fun CoroutineScope.buildJobs(channel: ReceiveChannel<Long>): ReceiveChannel<FilesToArchive> = produce {
        val itemSize = size * (1024.0.pow(units)).roundToLong()
        val timestamp = Instant.now().toEpochMilli()
        for (job in channel) {
            send(
                FilesToArchive(
                    LongRange(1, number)
                        .map { file ->
                            makeFileToArchive(job, file, timestamp, itemSize)
                        }.toList()
                )
            )
        }
    }

    private fun makeFileToArchive(job: Long, file: Long, timestamp: Long, size: Long): FileToArchive {
        return FileToArchive(
            "$job-$file-$timestamp",
            URI.create("aToZSequence://f"),
            size
        )
    }

    fun CoroutineScope.sendJobs(channel: ReceiveChannel<FilesToArchive>): ReceiveChannel<RioJob> = produce {
        val mutex = Mutex()
        var tokenExpire = Instant.now().plusSeconds(3000)
        var rioClient = createRioClient(
            host,
            createRioAuthClient(host).createToken(UserLoginCredentials("spectra", "spectra")).token
        )
        coroutineScope {
            repeat(threads) {
                launch {
                    for (files in channel) {
                        if (tokenExpire.isBefore(Instant.now())) {
                            try {
                                mutex.lock()
                                tokenExpire = Instant.now().plusSeconds(3000)
                                rioClient = createRioClient(
                                    host,
                                    createRioAuthClient(host).createToken(
                                        UserLoginCredentials(
                                            "spectra",
                                            "spectra"
                                        )
                                    ).token
                                )
                            } finally {
                                mutex.unlock()
                            }
                        }
                        send(rioClient.archiveFiles(broker, files))
                    }
                }
            }
        }
    }

}