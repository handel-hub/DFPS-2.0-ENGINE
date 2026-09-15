const pidusage = require('pidusage');

async function test() {
    console.log('Testing pidusage for pid', process.pid);
    const result = await pidusage(process.pid);
    console.log('Result:', result);
}

test().catch(console.error);
