import { defineWorkspace } from 'vitest/config';

export default defineWorkspace([
  {
    test: {
      name: 'unit',
      include: ['packages/**/*.test.ts', 'services/**/*.test.ts'],
    },
  },
  {
    test: {
      name: 'integration',
      include: ['tests/integration/**/*.test.ts'],
      testTimeout: 60_000,
      hookTimeout: 60_000,
      fileParallelism: false,
    },
  },
]);
