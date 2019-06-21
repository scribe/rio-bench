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
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
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
        runBlocking {
            val timedRioClient = TimedRioClient(host, UserLoginCredentials("spectra", "spectra"))
            val itemSize: Long = size * (1024.0.pow(units)).roundToLong()
            (0 until jobs).map { job ->
                (0 until number).map { file ->
                    FileToArchive(
                        "$job-$file-${Instant.now().toEpochMilli()}",
                        URI.create("aToZSequence://f"),
                        itemSize
                    )
                }.toList()
            }
                .map { fileToArchive ->
                    GlobalScope.launch (IO) {
                        timedRioClient.rioClient.archiveFiles(broker, FilesToArchive(fileToArchive))
                    }
                }
                .joinAll()
        }
        echo("Finished sending jobs")
    }

    inner class TimedRioClient(private val host: String, private val credentials: UserLoginCredentials) {
        private var now: Instant = Instant.now()
        var rioClient: RioClient =
            runBlocking { createRioClient(host, createRioAuthClient(host).createToken(credentials).token) }
            get() {
                return if (Instant.now().isAfter(now.plusSeconds(3000))) {
                    now = Instant.now()
                    field =
                        runBlocking { createRioClient(host, createRioAuthClient(host).createToken(credentials).token) }
                    field
                } else {
                    field
                }
            }
    }
}