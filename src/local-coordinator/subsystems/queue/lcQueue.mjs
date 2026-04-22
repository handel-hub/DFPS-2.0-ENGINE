
class LcQueue{
    #MAX_QUEUE_DEPTH
    #toDelete
    #lcQueue
    #idsQueue
    #toSend
    constructor(lcConfig={}) {
        this.#MAX_QUEUE_DEPTH=Number(lcConfig.MAX_QUEUE_DEPTH??30.0);
        this.#toDelete=[];
        this.#lcQueue=new Map();
        this.#idsQueue=[]
        this.#toSend=new Map()
    }

    add(data){
        for (const element of data) {
            if (this.#lcQueue.has(element.id)) {
                const check=this.#lcQueue.get(element.id)
                if (check.file_id===element.file_id) {
                    return
                }
            }
            this.#lcQueue.set(element.id,element)
            this.#idsQueue.push(element.id)
        }
    }
    update(id,data){
        if (!this.#lcQueue.has(id)) {
            return
        }
        const jobData=this.#lcQueue.get(id)
        if (!this.#toSend.has(id)) {
            this.#toSend.set(id,{})
        }
        const sendData=this.#toSend.get(id)
        for (const element of Object.keys(data)) {
            jobData[element]=data[element]
            sendData[element]=data[element]
        }
    }
    del(id){

        this.#toDelete.push(id)
        if (this.#toSend.has(id)) {
            
        }
    }

}