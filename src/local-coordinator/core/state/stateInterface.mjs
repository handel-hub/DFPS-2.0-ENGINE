import { 
    CpuProfileManager,
    IOProfile,
    JobStateRegistry,
    MemoryProfileStore,
    TimeProfileManager } from "./index.mjs";



class StateInterface {
        
    #Cpu;
    #Io;
    #Register;
    #Memory;
    #Time

    constructor() {
        this.#Cpu = new CpuProfileManager()
        this.#Io = new IOProfile()
        this.#Register = new JobStateRegistry()
        this.#Memory = new MemoryProfileStore()            
        this.#Time = new TimeProfileManager()
    }

    #getTime(){
        return this.#Time.getTimeProfile(pluginId,extension,fileSize,context)
    }

    #getCpu(pluginId,extension,context){
        return this.#Cpu.getCpuProfile(pluginId,extension,context)
    }

    #getMemory(){
        return this.#Memory.estimateRequiredMB(pluginId,extension,fileSizeBytes)
    }

    #getIO(pluginId,extension,context,s_in,s0){
        context.pluginId=pluginId;
        context.extension=extension

        return this.#Io.predict(context,s_in,s0)
    }
}