package pawsey.plugin

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

/** Creates the Setonix usage observer for each Nextflow session. */
@CompileStatic
class NfSetonixFactory implements TraceObserverFactory {
    @Override
    Collection<TraceObserver> create(Session session) {
        [new NfSetonixObserver(session)] as Collection<TraceObserver>
    }
}
