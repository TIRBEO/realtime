import { RT_PROTOCOL_VERSION } from '@/lib/protocol';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

const endpoints = [
  { path: '/ws', desc: 'WebSocket upgrade (wss://ws.tirbeo.app/ws)' },
  { path: '/api/health', desc: 'Health check' },
  { path: '/api/metrics', desc: 'Operational metrics (Bearer API_TOKEN)' },
  { path: '/api/publish', desc: 'HTTP event publishing (Bearer API_TOKEN)' },
];

export default function Home() {
  return (
    <main style={{ maxWidth: 720, margin: '0 auto', padding: '48px 24px' }}>
      <h1 style={{ fontSize: 24 }}>Tirbeo Realtime Platform</h1>
      <p style={{ opacity: 0.8 }}>
        Protocol v{RT_PROTOCOL_VERSION} · WebSocket endpoint <code>wss://ws.tirbeo.app/ws</code>
      </p>
      <table style={{ width: '100%', borderCollapse: 'collapse', marginTop: 24 }}>
        <thead>
          <tr>
            <th style={{ textAlign: 'left', borderBottom: '1px solid #2a2f3a', padding: 8 }}>Endpoint</th>
            <th style={{ textAlign: 'left', borderBottom: '1px solid #2a2f3a', padding: 8 }}>Description</th>
          </tr>
        </thead>
        <tbody>
          {endpoints.map((e) => (
            <tr key={e.path}>
              <td style={{ borderBottom: '1px solid #2a2f3a', padding: 8 }}>
                <code>{e.path}</code>
              </td>
              <td style={{ borderBottom: '1px solid #2a2f3a', padding: 8 }}>{e.desc}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  );
}
