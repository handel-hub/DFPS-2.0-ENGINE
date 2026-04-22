import { spawn } from "node:child_process";

const child=spawn('python',['./python.py'])


child.stdout.on('data',(data)=>{
    console.log('node is logging :',data.toString().trim())
})
child.stdout.on('end',()=>{
    console.log('finished this sequence logging')
})
child.stdout.on('close',()=>{
    clearTimeout(time)
    console.log('sucessfully closed')
})
child.stderr.on('error',(err)=>{
    console.log(err)
})
child.stderr.on('data',(err)=>{
    console.log(err.toString().trim())
})
child.on('close',()=>{
    console.log('sucessfully closed program')
})
child.on('exit',()=>console.log('exited'))

const int=setInterval(()=>{
    child.stdin.write(JSON.stringify({ command: "START_PROCESSING" }) + "\n")
},400)

const time=setTimeout(()=>{
    clearInterval(int)
    child.stdin.write(JSON.stringify({ command: "QUIT" }) + "\n")
},5000)
