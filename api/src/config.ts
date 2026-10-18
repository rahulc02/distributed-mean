import { z } from 'zod';

/** Every setting comes from the environment and is validated at startup. */
const Env = z.object({
  PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_URL: z.string().min(1),
  STORAGE_ROOT: z.string().default('/data'),
  GEN_THREADS: z.coerce.number().int().min(1).default(4),
  GEN_RANGE_SIZE: z.coerce.number().int().min(1).default(100),
  MAX_WORKERS: z.coerce.number().int().min(0).default(32),
  RABBIT_ADMIN_URL: z.string().url().default('http://rabbitmq:15672'),
  RABBIT_ADMIN_USER: z.string().default('dmean'),
  RABBIT_ADMIN_PASS: z.string().default('dmean'),
  UI_DIR: z.string().default('../ui'),
});

export type Config = z.infer<typeof Env>;

export const loadConfig = (env: NodeJS.ProcessEnv = process.env): Config => Env.parse(env);
