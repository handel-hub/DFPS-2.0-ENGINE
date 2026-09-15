const { spawn } = require('child_process');
const path = require('path');

const pluginPath = path.resolve('src/profilePlugin/tests/simulation/fib_plugin.js');
const child = spawn('node', [pluginPath]);

child.stdout.on('data', data => console.log('STDOUT:', data.toString()));
child.stderr.on('data', data => console.error('STDERR:', data.toString()));
child.on('close', code => console.log('Exited with', code));

const payload = JSON.stringify({
    action: 'PROCESS_FILE',
    taskId: '123',
    dataset_file: 'src/profilePlugin/tests/simulation/datasets/fib_small/1.txt',
    plugin_id: 'fib'
}) + '\n';

child.stdin.write(payload);
