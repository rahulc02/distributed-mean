import { Router } from 'express';
import { z } from 'zod';
import type { WorkerService } from '../../services/workerService.js';

const SetWorkers = z.object({ count: z.number().int().min(0) });

export function workerRoutes(workers: WorkerService): Router {
  const r = Router();

  r.get('/', async (_req, res) => {
    res.json(await workers.view());
  });

  /** Desired state: returns 202 at once; the supervisor converges within seconds. */
  r.put('/', async (req, res) => {
    const { count } = SetWorkers.parse(req.body);
    await workers.setDesired(count);
    res.status(202).json({ desired: count });
  });

  return r;
}
