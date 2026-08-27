if (process.argv.includes('--ignore-term')) {
  process.on('SIGTERM', () => {
    // Intentionally ignore graceful shutdown to exercise SIGKILL escalation.
  });
}

console.log('ready');
setInterval(() => {}, 1000);
