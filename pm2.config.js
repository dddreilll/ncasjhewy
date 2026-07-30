module.exports = {
  apps: [
    {
      name: 'fusion-cdh-store-consumer',
      script: 'dist/main.js',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      error_file: './logs/fusion-cdh-store-consumer-error.log',
      out_file: './logs/fusion-cdh-store-consumer-out.log',
      log_file: './logs/fusion-cdh-store-consumer-combined.log',
      time: true,
    },
  ],
};
