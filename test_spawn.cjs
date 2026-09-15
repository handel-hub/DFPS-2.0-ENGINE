const { spawn } = require('child_process');
const child = spawn(process.execPath, ['-e', 'console.log("hello")']);
child.on('spawn', () => console.log('Spawned!'));
child.stdout.on('data', d => console.log(d.toString()));
