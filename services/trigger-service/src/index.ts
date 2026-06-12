import { createApp } from './api/app';
import { startCronPoller } from './cron/poller';

const port = Number(process.env.TRIGGER_SERVICE_PORT ?? 3000);
createApp().listen(port, () => {
  console.log(`trigger-service listening on :${port}`);
  startCronPoller();
});
