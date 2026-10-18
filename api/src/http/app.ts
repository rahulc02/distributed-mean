import express, { type NextFunction, type Request, type Response } from 'express';
import { resolve } from 'node:path';
import { ZodError } from 'zod';
import type { Hub } from '../dashboard/hub.js';
import type { JobService } from '../services/jobService.js';
import type { WorkerService } from '../services/workerService.js';
import { BadRequestError, ConflictError, NotFoundError } from './errors.js';
import { eventRoutes } from './routes/events.js';
import { jobRoutes } from './routes/jobs.js';
import { workerRoutes } from './routes/workers.js';

export interface AppDeps {
  jobs: JobService;
  workers: WorkerService;
  hub: Hub;
  uiDir: string;
}

export function buildApp({ jobs, workers, hub, uiDir }: AppDeps): express.Express {
  const app = express();
  app.use(express.json({ limit: '16kb' }));

  app.get('/health', (_req, res) => {
    res.json({ ok: true });
  });
  app.use('/jobs', jobRoutes(jobs));
  app.use('/workers', workerRoutes(workers));
  app.use('/events', eventRoutes(hub));
  app.use(express.static(resolve(uiDir))); // the dashboard

  // The one error handler: Express 5 forwards rejected async handlers here.
  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof ZodError) return res.status(400).json({ error: 'invalid request', issues: err.issues });
    if (err instanceof BadRequestError) return res.status(400).json({ error: err.message });
    if (err instanceof NotFoundError) return res.status(404).json({ error: err.message });
    if (err instanceof ConflictError) return res.status(409).json({ error: err.message });
    console.error(JSON.stringify({ level: 'ERROR', service: 'api', msg: 'unhandled', err: String(err) }));
    return res.status(500).json({ error: 'internal error' });
  });

  return app;
}
