import { runOnce } from "./coordinator/mainCoordinator.mjs";

async function start() {
    try{
        await runOnce()
    }catch(err){
        console.error('Engine error',err)
    }finally{
        process.exit(0)
    }
}
start()
//console.log("DATABASE_URL:", process.env.DATABASE_URL);

