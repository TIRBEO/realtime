import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // ioredis/pg/ws use Node built-ins (net, tls) and must be required at runtime.
  serverExternalPackages: ['ioredis', 'pg', 'ws'],
};

export default nextConfig;
