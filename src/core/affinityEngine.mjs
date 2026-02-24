import {serialize,deserialize} from "v8";
class Affinity {
    #numSegment;
    #segmentRange;
    #variance_penalty_lambda;
    #ema_alpha;
    #min_sample_threshold;
    #temporal_decay_tau_seconds;
    #prior_alpha;
    #prior_beta;
    #adaptation_rate_rho
    #dataStructure;
    constructor(sizeSegmentation={},policyConfig={}) {

        this.#numSegment=sizeSegmentation.numSegment;
        this.#segmentRange=sizeSegmentation.segmentRange;
        
        this.#variance_penalty_lambda=policyConfig.variance_penalty_lambda||0.75;
        this.#ema_alpha=policyConfig.ema_alpha||0.2;
        this.#min_sample_threshold=policyConfig.min_sample_threshold||50;
        this.#temporal_decay_tau_seconds=policyConfig.temporal_decay_tau_seconds||86400;
        this.#prior_alpha=policyConfig.prior_alpha||2;
        this.#prior_beta=policyConfig.prior_beta||2;
        this.#adaptation_rate_rho=policyConfig.adaptation_rate_rho||0.02
        
        this.#dataStructure=new Map()
        
        this.#initialize()

    }
    #initialize(){
        for (let index = 0; index < this.#numSegment; index++) {
            const range=this.#segmentRange[index]
            const rangeName=Object.keys(range)[0]
            this.#dataStructure.set(rangeName,new Map())
        }

    }
    #varibles(){
        return{
            rawStats:{
                time:0,
                size:0,
                successIndicator:0||1,
                timeStamp:0,
                totalS:0,
                totalN:0
            },
            derived:{
                emaTime:0,
                deviation:0,
                reliabiltySuccess:0,
                reliabiltyTotal:0
            },
            result:0
        }
    }
    #bucketSegment(size){
        let bucket=''
        this.#segmentRange.filter(elem=>{
            const keys=Object.keys(elem)[0]
            const min=elem[keys].min
            const max=elem[keys].max
            if (size>=min&&size<=max) {
                bucket=keys
            }
        })
        return bucket
    }
    #updateAffinity(id,value,pipeline){

        const {time, size, successIndicator, timeStamp}=value

        const bucket=this.#bucketSegment(size)
        const pipelineSegment=this.#dataStructure.get(bucket)
        if (!pipelineSegment.has(pipeline)) {
            pipelineSegment.set(pipeline,new Map())
        }
        const lcSegment=pipelineSegment.get(pipeline)
        if (!lcSegment.has(id)) {
            const variables=this.#varibles()
            lcSegment.set(id,variables)
        }

        const segment=lcSegment.get(id)
        
        const execution=this.#executionTime(size,time)
        const emaExec=this.#emaExecution(execution,segment.derived.emaTime)
        const emaDevi=this.#emaDeviation(execution,emaExec,segment.derived.deviation)
        if (successIndicator===1){
            const old=segment.rawStats.totalS
            const current=segment.rawStats.totalS+1
            const success=this.#emaSuccess(current,old)
            segment.rawStats.totalS=current
            segment.derived.reliabiltySuccess=success
        }
        const reliabiltyT=this.#emaTotal(segment.rawStats.totalN)
        segment.derived.reliabiltyTotal+=1
        segment.derived.emaTime=emaExec
        segment.derived.deviation=emaDevi
        segment.derived.reliabiltyTotal=reliabiltyT
        
        segment.rawStats.totalN+=1
        segment.rawStats.size=size
        segment.rawStats.time=time
        segment.rawStats.successIndicator=successIndicator
        segment.rawStats.timeStamp=timeStamp

    }
    runUpdateAffinty(value){
        const id=value.id;
        value.data.forEach(element => {
            const pipeline=element.pipeline
            const value=element.value
            this.#updateAffinity(id,value,pipeline)
        });

    }
    #executionTime(size,time){
        return time/size
    }
    #emaExecution(execution,old){
        return (this.#ema_alpha*execution)+((1-this.#ema_alpha)*old)
        
    }
    #emaDeviation(execution,ema,old){
        const absolute=Math.abs(execution-ema)
        return this.#ema_alpha*absolute+((1-this.#ema_alpha)*old)
    }
    #emaSuccess(current,old){
        return (this.#adaptation_rate_rho*current)+((1-this.#adaptation_rate_rho)*old)
    }
    #emaTotal(old){
        return this.#adaptation_rate_rho+((1-this.#adaptation_rate_rho)*old)
    }
    #perf(emaTime,deviation){
        return 1/(emaTime+(deviation*this.#variance_penalty_lambda)+0.0000001)
    }
    #reliabilty(s,n){
        return (s+this.#prior_alpha)/(n+this.#prior_alpha+this.#prior_beta)
    }
    #structuralConfidence(totalN,timestamp){
        const delta=(Date.now()-timestamp)
        const temporalDecay=Math.exp(-delta/this.#temporal_decay_tau_seconds)
        return (totalN/(totalN+this.#min_sample_threshold))*temporalDecay
    }
    #calculateResult(emaTime,deviation,s,n,totalN,timestamp){
        return this.#perf(emaTime,deviation)*this.#reliabilty(s,n)*this.#structuralConfidence(totalN,timestamp)
    }
    getAffinity(){
        for (let pipelines of this.#dataStructure.values()) {
        for (let  ids of pipelines) {
            for (let  data of ids) {

                data.result=this.#calculateResult(
                    data.derived.emaTime,
                    data.derived.deviation,
                    data.derived.reliabiltySuccess,
                    data.derived.reliabiltyTotal,
                    data.rawStats.totalN,
                    data.rawStats.timestamp
                )
            }
        }
    }


        return deserialize(serialize(this.#dataStructure))
    }
}



/* 
    N= totalSamples
    Nmin= min number for jobs per type
    S=success Rate

*/

/* 
{
    "affinity": {
        "performance": {
        "variance_penalty_lambda": 0.75,
        "ema_alpha": 0.2
        },
        "confidence": {
        "min_sample_threshold": 50,
        "temporal_decay_tau_seconds": 86400
        },
        "reliability": {
        "prior_alpha": 2,
        "prior_beta": 2,
        "adaptation_rate_rho": 0.02
        },
        "redemption": {
        "conservative_mode": true
        }
    }
}
*/

/* 
reanlyzing to restucture rare file buckets

to instatiate with one more bucket if the file type sees it fit that will be complexity
class for a more granular segmentation and information this wiggle room for this 
robustness will be done on later days.

variance penalty affected by lamda


room for improvemnts properly modelling O(n log(n) and O(n^2) more of
constant overhead processing + linear processingpresent system models workloads 
that scales linearly. example of scenarior heavily compressed file and raw file.
i reccomend segmenting the complexity bucket by file type, compression type and
modality, regresional analysis could be done.

system model profers a solution that helps reduce the effect of the issues above
without formaly addressing the model, by usingsegmentation by file size this is 
default aim is to reduce the variance score without applying regression.
i was quite cormfortable with this system. if during your testing and and result are 
indesirable  you could consider the previous reccomendation above.
another reason for not going the regresion route is such a solution will be undesirable
in node js  perhaps python, c++, rust, java, go will produce more desirable result

explicit temporal decay
two layers of application is at structural confidence and reliability estimate.
structural confidence is an epistemic certinty which is decayed explicitly this is tunable 
at the policy config.
reliability decay can be applied too but should be at a much more slower rate as the default
confidence temporal decay (i.e success rate decays temporally)
performance already decays implictly temporally through EMA

system does not incorporate reliability decay

desired Redemption Latency for a node:
redemption of nodes that begin to show positive signs of improvemnts after series of bad performance
is set to be slow and this was done by default because the system is assmed to be conservatve.

affnity does not factor latency of any sort 'infrastrutucre'(disk failures or shared resource contention)
affnity is merely behavioural infrastructure degradation affect the system it does not include any model 
to mitigate such issues of any sort. node redemption latency becomes the major draw back. but this is
tolerable as the system improves with time 


affinity incorporate full processing pipeline because that is desirable but stage level affinity becomes
necesssary if pipeline crosses over differnt hardware path (cpu and gpu), stages are independently scheduled
and many more scenerios not listed.



*/

