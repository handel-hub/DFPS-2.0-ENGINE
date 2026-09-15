import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATASETS_DIR = path.join(__dirname, 'src', 'profilePlugin', 'tests', 'simulation', 'datasets');

async function createDataset(name, count, min, max) {
    const dirPath = path.join(DATASETS_DIR, name);
    await fs.mkdir(dirPath, { recursive: true });
    
    for (let i = 1; i <= count; i++) {
        const num = Math.floor(Math.random() * (max - min + 1)) + min;
        const filePath = path.join(dirPath, `data_${i}.txt`);
        await fs.writeFile(filePath, num.toString() + '\n');
    }
    console.log(`Created dataset ${name} with ${count} files (range ${min}-${max})`);
}

async function main() {
    await createDataset('fib_small', 10, 25, 30);
    await createDataset('fib_medium', 20, 30, 35);
    await createDataset('fib_large', 5, 38, 45); // Max 45 as requested by user
    console.log('Dataset generation complete.');
}

main().catch(console.error);
