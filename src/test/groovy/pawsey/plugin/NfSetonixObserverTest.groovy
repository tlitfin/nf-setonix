package pawsey.plugin

import groovy.util.ConfigObject
import nextflow.Session
import spock.lang.Specification

class NfSetonixObserverTest extends Specification {

    def 'should create the observer instance'() {
        given:
        Session session = Mock(Session) {
            getConfig() >> new ConfigObject()
        }

        when:
        def result = new NfSetonixFactory().create(session)

        then:
        result.size() == 1
        result.first() instanceof NfSetonixObserver
    }

    def 'should parse Setonix CPU sacct output'() {
        given:
        String output = '''\
12345|work|32|01:30:00|5400|02:00:00||64Gn|COMPLETED|0:0|1|billing=36,cpu=32,mem=64G,node=1|billing=36,cpu=32,mem=64G,node=1|01:00:00|
12345.batch||32|01:30:00|5400||61G||COMPLETED|0:0|1|cpu=32,mem=64G,node=1||01:00:00|
12345.extern||32|01:30:00|5400|||2M|COMPLETED|0:0|1|cpu=32,mem=64G,node=1||00:00:02|
'''

        when:
        Map values = NfSetonixObserver.parseSacct(output, '12345')

        then:
        values.partition == 'work'
        values.sus == '54.00'
        values.cpus == '32'
        values.cpuTime == '01:00:00'
        values.memory == '61G'
        values.gpus == '0'
        values.nodes == '1'
        values.efficiency == '0.02'
        values.state == 'COMPLETED'
        values.exitCode == '0:0'
    }

    def 'should parse GPU TRES and aggregate step CPU time'() {
        given:
        String output = '''\
67890|gpu|16|00:30:00|1800|01:00:00||2Gc|FAILED|1:0|1|billing=128,cpu=16,gres/gpu=2,mem=32G,node=1|cpu=16,gres/gpu=2,mem=32G,node=1||
67890.batch||16|00:30:00|1800||512M||FAILED|1:0|1|||00:01:00|
67890.0||16|00:29:50|1790||20G||FAILED|1:0|1|||02:00:00|
'''

        expect:
        with(NfSetonixObserver.parseSacct(output, '67890')) {
            sus == '64.00'
            gpus == '2'
            memory == '20G'
            cpuTime == '02:01:00'
            state == 'FAILED'
            exitCode == '1:0'
        }
    }

    def 'should fall back to the published Setonix charging formula'() {
        given:
        String output = '''\
24680|highmem|32|04:00:00|14400|05:00:00||490Gn|COMPLETED|0:0|1|cpu=32,mem=490G,node=1|cpu=32,mem=490G,node=1|01:00:00|
'''

        expect:
        NfSetonixObserver.parseSacct(output, '24680').sus == '256.00'
    }

    def 'should parse Slurm durations and memory units'() {
        expect:
        NfSetonixObserver.durationSeconds('1-02:03:04') == 93784d
        NfSetonixObserver.memoryBytes('1.5G') == 1610612736L
        NfSetonixObserver.memoryBytes('512Mc') == 536870912L
    }

    def 'should return no values when the allocation row is absent'() {
        expect:
        NfSetonixObserver.parseSacct('123.0|work|1|00:00:01|1|00:01:00||||0:0|1||||\n', '123') == [:]
    }
}
