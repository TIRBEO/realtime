import type { Metadata } from 'next';
import type { ReactNode } from 'react';

export const metadata: Metadata = {
  title: 'Tirbeo Realtime',
  description: 'Tirbeo Realtime Platform — centralized WebSocket infrastructure (ws.tirbeo.app)',
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body style={{ margin: 0, fontFamily: 'system-ui, -apple-system, sans-serif', background: '#0b0e14', color: '#e6e8ee' }}>
        {children}
      </body>
    </html>
  );
}
