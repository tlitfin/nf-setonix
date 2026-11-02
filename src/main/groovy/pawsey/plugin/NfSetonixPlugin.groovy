package pawsey.plugin

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper

/** The nf-setonix plugin entry point. */
@CompileStatic
class NfSetonixPlugin extends BasePlugin {
    NfSetonixPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }
}
