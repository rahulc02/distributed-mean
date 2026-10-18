/**
 * Composition root: the only file that knows concrete classes. Everything else
 * receives its dependencies, so swapping an adapter is a one-line change here.
 */
import pg from 'pg';
import { loadConfig } from './config.js';
import { Hub } from './dashboard/hub.js';
import { Poller } from './dashboard/poller.js';
import { RabbitAdmin } from './dashboard/rabbitAdmin.js';
import { Generator } from './generation/generator.js';
import { buildApp } from './http/app.js';
import { EventRepo } from './repos/eventRepo.js';
import { JobRepo } from './repos/jobRepo.js';
import { WorkerRepo } from './repos/workerRepo.js';
import { JobService } from './services/jobService.js';
import { WorkerService } from './services/workerService.js';
import { LocalStore } from './storage/localStore.js';

const config = loadConfig();
const db = new pg.Pool({ connectionString: config.DATABASE_URL, max: 10 });

const store = new LocalStore(config.STORAGE_ROOT);
const events = new EventRepo(db);
const jobRepo = new JobRepo(db);
const generator = new Generator(config.GEN_THREADS, config.STORAGE_ROOT, config.GEN_RANGE_SIZE);
const jobs = new JobService(jobRepo, events, generator, store);
const workers = new WorkerService(new WorkerRepo(db), events, config.MAX_WORKERS);
const hub = new Hub();
const rabbit = new RabbitAdmin(config.RABBIT_ADMIN_URL, config.RABBIT_ADMIN_USER, config.RABBIT_ADMIN_PASS);
const poller = new Poller(jobRepo, events, workers, rabbit, hub);

const server = buildApp({ jobs, workers, hub, uiDir: config.UI_DIR }).listen(config.PORT, () => {
  console.log(JSON.stringify({ level: 'INFO', service: 'api', msg: `listening on ${config.PORT}` }));
});

await jobs.resumeGenerating(); // jobs left mid-generation by a previous run
await poller.start();

const shutdown = async () => {
  poller.stop();
  server.close();
  await jobs.close();
  await db.end();
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
