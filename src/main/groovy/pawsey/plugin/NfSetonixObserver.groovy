/*
 * Copyright 2026, Pawsey Supercomputing Research Centre
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package pawsey.plugin

import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.file.FileHelper
import nextflow.processor.TaskHandler
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap

/**
 * Produces a per-process Slurm usage report for Nextflow runs on Setonix.
 *
 * Resource usage is read from Slurm's accounting database with sacct. The
 * command is invoked directly (without a shell) and job IDs are validated
 * before use.
 */
@Slf4j
class NfSetonixObserver implements TraceObserver {

    private static final String SACCT_FIELDS = [
        'JobIDRaw', 'Partition', 'AllocCPUS', 'Elapsed', 'ElapsedRaw',
        'Timelimit', 'MaxRSS', 'ReqMem', 'State', 'ExitCode', 'NNodes',
        'AllocTRES', 'ReqTRES', 'TotalCPU'
    ].join(',')

    private static final List<String> CSV_HEADERS = [
        'Name', 'Process', 'Partition', 'Service Units', 'CPUs', 'CPU time',
        'Used Walltime', 'Requested Walltime', 'Used Memory',
        'Requested Memory', 'GPUs', 'Nodes', 'Efficiency', 'State', 'Exit Code'
    ]

    private final Session session
    private final Object reportLock = new Object()

    private String format
    private String output
    private Path path
    private Object publishTo
    private String sacctCommand
    private int accountingRetries
    private long accountingPollMillis

    final Map<String, List<Map<String, String>>> results = new ConcurrentHashMap<>()

    NfSetonixObserver(Session session) {
        this.session = session
        this.format = configValue('setonix.format', '').toString()
        this.output = configValue('setonix.output', '').toString()
        this.publishTo = configValue('setonix.publishTo', '')
        this.sacctCommand = configValue('setonix.sacct', 'sacct').toString()
        this.accountingRetries = integerConfig('setonix.accountingRetries', 12)
        this.accountingPollMillis = longConfig('setonix.accountingPollMillis', 10_000L)
    }

    private Object configValue(String key, Object defaultValue) {
        session.config.navigate(key, defaultValue)
    }

    private int integerConfig(String key, int defaultValue) {
        try {
            return Math.max(1, configValue(key, defaultValue).toString().toInteger())
        }
        catch (Exception ignored) {
            log.warn "Invalid $key setting; using $defaultValue"
            return defaultValue
        }
    }

    private long longConfig(String key, long defaultValue) {
        try {
            return Math.max(0L, configValue(key, defaultValue).toString().toLong())
        }
        catch (Exception ignored) {
            log.warn "Invalid $key setting; using $defaultValue"
            return defaultValue
        }
    }

    void addToResults(String key, List<Map<String, String>> entries) {
        results.compute(key) { ignored, existing ->
            List<Map<String, String>> target = existing ?: new ArrayList<Map<String, String>>()
            synchronized (target) {
                target.addAll(entries)
            }
            target
        }
    }

    void configCheck() {
        format = format?.trim()?.toLowerCase()
        output = output?.trim()

        if (!format && !output) {
            format = 'csv'
            output = 'UsageReport.csv'
        }
        else if (!format) {
            format = output.toLowerCase().endsWith('.json') ? 'json' :
                output.toLowerCase().endsWith('.csv') ? 'csv' : ''
        }
        else if (!output) {
            output = format == 'json' ? 'UsageReport.json' : 'UsageReport.csv'
        }

        if (!(format in ['csv', 'json'])) {
            throw new IllegalArgumentException("setonix.format must be 'csv' or 'json'")
        }
        if (!output.toLowerCase().endsWith(".${format}")) {
            throw new IllegalArgumentException("setonix.output must end in .${format}")
        }

        path = Paths.get(output)
        Path parent = path.toAbsolutePath().parent
        if (parent != null) Files.createDirectories(parent)
        log.info "Setonix usage report will be saved to $path"
    }

    @Override
    void onFlowBegin() {
        configCheck()
        if (format == 'csv') {
            Files.write(path, (CSV_HEADERS.collect { csvEscape(it) }.join(',') + '\n').bytes)
        }
    }

    @Override
    void onFlowComplete() {
        if (format == 'json') {
            Files.write(path, JsonOutput.prettyPrint(JsonOutput.toJson(results)).bytes)
        }

        String destination = resolvePublishTo(publishTo)
        if (destination) {
            try {
                Path dest = FileHelper.asPath(destination) as Path
                Files.copy(path, dest, StandardCopyOption.REPLACE_EXISTING)
                log.info "Usage report published to $dest"
            }
            catch (Exception e) {
                log.warn "Failed to publish usage report to '$destination': ${e.message}"
            }
        }
        log.info 'Setonix usage report complete'
    }

    @Override
    void onProcessCached(TaskHandler handler, TraceRecord trace) {
        recordProcess(handler, trace, false)
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        recordProcess(handler, trace, true)
    }

    private void recordProcess(TaskHandler handler, TraceRecord trace, boolean retry) {
        String processName = handler.task.name
        String jobId = traceValue(trace, 'native_id')?.trim()
        Map<String, String> values = [:]

        if (validJobId(jobId)) {
            values = queryAccounting(jobId, retry ? accountingRetries : 1)
        }
        else {
            log.warn "No valid Slurm job ID was recorded for process '$processName'"
        }

        // Preserve useful Nextflow trace values if Slurm accounting is delayed or unavailable.
        values.partition = values.partition ?: traceValue(trace, 'queue')
        values.exitCode = values.exitCode ?: traceValue(trace, 'exit')
        values.name = processName

        if (format == 'csv') appendCsv(processName, values)
        addToResults(processName.split(/\s+/)[0], [values])
    }

    private static boolean validJobId(String value) {
        value != null && value ==~ /\d+(?:_\d+)?(?:\+\d+)?/
    }

    protected Map<String, String> queryAccounting(String jobId, int attempts) {
        Map<String, String> values = [:]
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                String output = runSacct(jobId)
                values = parseSacct(output, jobId)
                if (values) return values
            }
            catch (Exception e) {
                log.warn "Unable to read Slurm accounting for job $jobId: ${e.message}"
                return [:]
            }

            if (attempt < attempts && accountingPollMillis > 0) {
                log.debug "Waiting for Slurm accounting record for job $jobId"
                sleep(accountingPollMillis)
            }
        }
        log.warn "No Slurm accounting record found for job $jobId after $attempts attempt(s)"
        values
    }

    protected String runSacct(String jobId) {
        List<String> command = [
            sacctCommand, '--noheader', '--parsable2', '--starttime', '1970-01-01',
            '--jobs', jobId, '--format', SACCT_FIELDS
        ]
        Process process = new ProcessBuilder(command).start()
        String stdout = process.inputStream.getText('UTF-8')
        String stderr = process.errorStream.getText('UTF-8')
        int status = process.waitFor()
        if (status != 0) {
            throw new IOException("$sacctCommand exited with status $status: ${stderr.trim()}")
        }
        stdout
    }

    /** Parse pipe-delimited sacct output and merge the allocation and job-step metrics. */
    static Map<String, String> parseSacct(String text, String jobId) {
        if (!text?.trim()) return [:]

        List<Map<String, String>> rows = text.readLines().findAll { it.trim() }.collect { line ->
            String[] fields = line.split('\\|', -1)
            if (fields.length < 14) return null
            [
                jobId: fields[0].trim(), partition: fields[1].trim(),
                cpus: fields[2].trim(), walltime: fields[3].trim(),
                elapsedSeconds: fields[4].trim(), requestedWalltime: fields[5].trim(),
                memory: fields[6].trim(), requestedMemory: fields[7].trim(),
                state: fields[8].trim(), exitCode: fields[9].trim(),
                nodes: fields[10].trim(), allocTres: fields[11].trim(),
                reqTres: fields[12].trim(), cpuTime: fields[13].trim()
            ]
        }.findAll { it != null }

        Map<String, String> allocation = rows.find { it.jobId == jobId }
        if (!allocation) return [:]

        List<Map<String, String>> steps = rows.findAll {
            it.jobId.startsWith(jobId + '.') && !it.jobId.endsWith('.extern')
        }
        String usedMemory = ([allocation.memory] + steps.collect { it.memory })
            .findAll { it }
            .max { a, b -> memoryBytes(a) <=> memoryBytes(b) }
        String cpuTime = allocation.cpuTime ?: steps.findAll { it.cpuTime }
            .collect { durationSeconds(it.cpuTime) }
            .sum()
            ?.with { formatDuration(it as long) }

        Map<String, String> tres = parseTres(allocation.allocTres ?: allocation.reqTres)
        String gpus = tres.findAll { key, ignored -> key == 'gres/gpu' || key.startsWith('gres/gpu:') }
            .values()
            .collect { numericValue(it) }
            .sum()
            ?.with { stripDecimal(it as double) }

        long elapsed = numericValue(allocation.elapsedSeconds).longValue()
        int cpus = numericValue(allocation.cpus).intValue()
        double cpuSeconds = durationSeconds(cpuTime)
        String efficiency = elapsed > 0 && cpus > 0
            ? String.format(Locale.ROOT, '%.2f', cpuSeconds / (elapsed * cpus))
            : ''

        [
            partition: allocation.partition,
            sus: serviceUnits(allocation, tres),
            cpus: allocation.cpus,
            cpuTime: cpuTime ?: '',
            walltime: allocation.walltime,
            requestedWalltime: allocation.requestedWalltime,
            memory: usedMemory ?: '',
            requestedMemory: allocation.requestedMemory,
            gpus: gpus ?: '0',
            nodes: allocation.nodes,
            efficiency: efficiency,
            state: allocation.state,
            exitCode: allocation.exitCode
        ]
    }

    private static String serviceUnits(Map<String, String> row, Map<String, String> tres) {
        double hours = numericValue(row.elapsedSeconds).doubleValue() / 3600d
        if (hours <= 0) return ''

        double billing = numericValue(tres.billing).doubleValue()
        if (billing > 0) return String.format(Locale.ROOT, '%.2f', billing * hours)

        // Fallback based on Pawsey's published Setonix accounting formula.
        int nodes = Math.max(1, numericValue(row.nodes).intValue())
        int cpus = numericValue(row.cpus).intValue()
        double memoryGiB = memoryBytes(tres.mem) / (1024d * 1024d * 1024d)
        String partition = row.partition
        double gpus = tres.findAll { key, ignored -> key == 'gres/gpu' || key.startsWith('gres/gpu:') }
            .values().collect { numericValue(it).doubleValue() }.sum() ?: 0d

        double unitsPerHour
        if (partition == 'quantum') {
            unitsPerHour = gpus > 0 ? 1024d * (gpus / (4d * nodes)) * nodes : 1024d * nodes
        }
        else if (partition?.contains('gpu')) {
            unitsPerHour = gpus > 0 ? 512d * (gpus / (8d * nodes)) * nodes : 512d * nodes
        }
        else {
            double memoryPerNode = partition == 'highmem' ? 980d : 230d
            double cpuProportion = cpus / (128d * nodes)
            double memoryProportion = memoryGiB / (memoryPerNode * nodes)
            unitsPerHour = 128d * Math.max(cpuProportion, memoryProportion) * nodes
        }
        String.format(Locale.ROOT, '%.2f', unitsPerHour * hours)
    }

    static Map<String, String> parseTres(String value) {
        Map<String, String> result = [:]
        value?.split(',')?.each { item ->
            int separator = item.indexOf('=')
            if (separator > 0) result[item.substring(0, separator)] = item.substring(separator + 1)
        }
        result
    }

    static long memoryBytes(String value) {
        if (!value) return 0L
        def matcher = value.trim() =~ /(?i)^(\d+(?:\.\d+)?)([KMGTPE]?)(?:i?B)?[nc]?$/
        if (!matcher.matches()) return 0L
        double amount = matcher.group(1).toDouble()
        String unit = matcher.group(2).toUpperCase()
        int power = ['', 'K', 'M', 'G', 'T', 'P', 'E'].indexOf(unit)
        (amount * Math.pow(1024d, Math.max(0, power))) as long
    }

    static double durationSeconds(String value) {
        if (!value) return 0d
        if (value ==~ /\d+(?:\.\d+)?/) return value.toDouble()
        def matcher = value =~ /^(?:(\d+)-)?(\d+):(\d+):(\d+(?:\.\d+)?)$/
        if (!matcher.matches()) return 0d
        double days = matcher.group(1) ? matcher.group(1).toDouble() : 0d
        days * 86400d + matcher.group(2).toDouble() * 3600d +
            matcher.group(3).toDouble() * 60d + matcher.group(4).toDouble()
    }

    private static Number numericValue(String value) {
        try { value ? value.toDouble() : 0d }
        catch (NumberFormatException ignored) { 0d }
    }

    private static String formatDuration(long seconds) {
        long days = seconds.intdiv(86400)
        long remainder = seconds % 86400
        String clock = String.format(Locale.ROOT, '%02d:%02d:%02d',
            remainder.intdiv(3600), (remainder % 3600).intdiv(60), remainder % 60)
        days ? "${days}-${clock}" : clock
    }

    private static String stripDecimal(double value) {
        value == Math.rint(value) ? String.valueOf(value as long) : String.valueOf(value)
    }

    private void appendCsv(String processName, Map<String, String> values) {
        List<Object> row = [
            processName, processName.split(/\s+/)[0], values.partition, values.sus,
            values.cpus, values.cpuTime, values.walltime, values.requestedWalltime,
            values.memory, values.requestedMemory, values.gpus, values.nodes,
            values.efficiency, values.state, values.exitCode
        ]
        synchronized (reportLock) {
            Files.write(path, (row.collect { csvEscape(it) }.join(',') + '\n').bytes,
                StandardOpenOption.APPEND)
        }
    }

    private static String csvEscape(Object raw) {
        String value = raw == null ? '' : raw.toString()
        if (value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r')) {
            return '"' + value.replace('"', '""') + '"'
        }
        value
    }

    private static String traceValue(TraceRecord trace, String key) {
        Object value = trace.get(key)
        value == null ? '' : value.toString()
    }

    private static String resolvePublishTo(Object raw) {
        Object value = raw instanceof Closure ? raw.call() : raw
        String result = value?.toString()?.trim()
        result ?: null
    }
}
