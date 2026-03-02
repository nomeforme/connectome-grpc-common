import * as Sentry from '@sentry/node';

export function initErrorTracking(options: {
  serviceName: string;
  dsn?: string;
  environment?: string;
}) {
  const dsn = options.dsn || process.env.SENTRY_DSN || process.env.GLITCHTIP_DSN;
  if (!dsn) {
    console.log(`[${options.serviceName}] Error tracking disabled (no DSN)`);
    return;
  }

  Sentry.init({
    dsn,
    environment: options.environment || process.env.NODE_ENV || 'production',
    serverName: options.serviceName,
    tracesSampleRate: 0.1,
    beforeSend(event) {
      event.tags = { ...event.tags, service: options.serviceName };
      return event;
    },
  });

  process.on('uncaughtException', (error) => {
    Sentry.captureException(error);
    console.error(`[${options.serviceName}] Uncaught exception:`, error);
  });
  process.on('unhandledRejection', (reason) => {
    Sentry.captureException(reason);
    console.error(`[${options.serviceName}] Unhandled rejection:`, reason);
  });

  console.log(`[${options.serviceName}] Error tracking enabled`);
}

export { Sentry };
