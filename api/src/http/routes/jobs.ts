import { Router } from 'express';
import { z } from 'zod';
import type { JobService } from '../../services/jobService.js';

/** The assignment's bounds: 1 < F < 100k, 1 < C < 1000k. */
export const CreateJob = z.object({
  f: z.number().int().min(2).max(99_999),
  c: z.number().int().min(2).max(999_999),
});

const JobId = z.string().uuid();

export function jobRoutes(jobs: JobService): Router {
  const r = Router();

  r.post('/', async (req, res) => {
    const id = await jobs.create(CreateJob.parse(req.body));
    res.status(202).location(`/jobs/${id}`).json({ id });
  });

  r.get('/:id', async (req, res) => {
    res.json(await jobs.get(JobId.parse(req.params.id)));
  });

  r.get('/:id/result', async (req, res) => {
    const id = JobId.parse(req.params.id);
    const stream = await jobs.result(id);
    res.type('text/csv').attachment(`${id}.csv`);
    stream.on('error', () => res.destroy());
    stream.pipe(res); // streamed: C can be a million values
  });

  return r;
}
