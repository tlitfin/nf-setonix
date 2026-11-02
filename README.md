# nf-setonix

`nf-setonix` creates a per-process resource-usage report for Nextflow workflows
running on the Pawsey Setonix supercomputer. Nextflow 24.04.1 or later is
required.

The plugin queries completed jobs through Slurm accounting with `sacct`.
Reports include the Slurm partition, estimated service units, allocated CPUs
and GPUs, CPU and wall time, requested and used memory, nodes, CPU efficiency,
job state, and exit code.

## Build and install

On a system with internet access:

```bash
git clone https://github.com/tlitfin/nf-setonix.git
cd nf-setonix

# Keep the installer and Nextflow runtime pointed at the same plugin directory.
module load nextflow/25.04.6

# On Setonix, keep the Gradle cache out of $HOME.
if [ -n "${MYSCRATCH:-}" ]; then
    export GRADLE_USER_HOME="$MYSCRATCH/.gradle"
fi

./gradlew check
make install
```

Be sure to load nextflow before installing the plugin to ensure installation in the location expected at run time.

Build and install the plugin before transferring it to Setonix if dependencies
cannot be downloaded there. Re-run `make install` after changing the plugin
source.

## Nextflow configuration

Enable the locally installed plugin in `nextflow.config`:

```groovy
plugins {
    id 'nf-setonix@0.1.0'
}
```

The default report is `UsageReport.csv`. The report format and destination can
be changed under the `setonix` scope:

```groovy
setonix {
    format = 'csv'                 // csv or json
    output = 'report.csv'
}
```

Do not change the report name between a run and its `-resume` invocation if the
cached and newly executed process records should be collected in the same file.
To keep a separate report for every invocation, include a timestamp:

```groovy
params.reportTimestamp = new Date().format('yyyy-MM-dd_HH-mm-ss')

setonix {
    output = "report_${params.reportTimestamp}.csv"
}
```

### Accounting settings

Normally no accounting configuration is needed: `sacct` is available in the
Setonix environment. Slurm can take a short time to commit a completed job to
its accounting database, so the plugin retries for up to two minutes by
default.

```groovy
setonix {
    sacct = 'sacct'                  // executable name or absolute path
    accountingRetries = 12
    accountingPollMillis = 10_000
}
```

Cached tasks are queried once. Very old cached jobs may no longer be retained
in Slurm accounting; such rows are still emitted with any values available in
the Nextflow trace.

### Service-unit calculation

The preferred calculation is:

```text
Slurm billing TRES × elapsed hours
```

This uses the billable resource value configured by Pawsey in Slurm. If the
billing TRES is not returned, the plugin applies Pawsey's documented Setonix
formula using the allocated CPU, memory, GPU, node, partition, and elapsed-time
fields. The result should be treated as an estimate; project-level usage should
be checked with `pawseyAccountBalance`.

### Publishing the finished report

`publishTo` copies the completed report to anything Nextflow can resolve as a
`Path`, including object-storage paths when the relevant Nextflow plugin and
credentials are configured:

```groovy
setonix {
    output = 'report.csv'
    publishTo = 's3://bucket/results/UsageReport.csv'
}
```

Closures are also supported for `publishTo`.

## Running

After installing and configuring the plugin:

```bash
nextflow run main.nf
```

It can also be selected on the command line:

```bash
nextflow run main.nf -plugins nf-setonix@0.1.0
```

The workflow processes themselves must use Nextflow's `slurm` executor and a
valid Setonix account/partition configuration. For GPU partitions, Pawsey
requires the GPU project account (normally the project code with `-gpu`).

## Acknowledgements

`nf-setonix` was adapted from the
[`nf-gadi`](https://github.com/eileen-xue/nf-gadi) Nextflow plugin. We thank the
`nf-gadi` authors and contributors for the original plugin structure and usage
reporting implementation on which this project was based.

## Setonix references

- [Setonix user guide](https://pawsey.atlassian.net/wiki/spaces/US/overview?homepageId=51904514)
- [Running jobs on Setonix](https://pawsey.atlassian.net/wiki/spaces/US/pages/51929058/Running+Jobs+on+Setonix)
- [Setonix general information and accounting](https://pawsey.atlassian.net/wiki/spaces/US/pages/51929028/Setonix+General+Information)
- [Pawsey job scheduling and `sacct`](https://pawsey.atlassian.net/wiki/spaces/US/pages/51925964/Job+Scheduling)
