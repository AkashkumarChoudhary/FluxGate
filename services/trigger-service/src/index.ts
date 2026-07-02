import { createApp } from './api/app';
import { startCronPoller } from './cron/poller';

// Prevent transient DB/Redis errors from async route handlers from crashing the process.
process.on('unhandledRejection', (reason) => {
  console.error('unhandledRejection (non-fatal):', reason);
});

const port = Number(process.env.TRIGGER_SERVICE_PORT ?? 3000);
createApp().listen(port, () => {
  console.log(`trigger-service listening on :${port}`);
  startCronPoller();
});
