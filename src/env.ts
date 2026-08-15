export interface Env {
  REALTIME_HUB: DurableObjectNamespace;
  JWT_SECRET?: string;
  API_TOKEN?: string;
  RT_NAMESPACE?: string;
  SUPABASE_URL?: string;
  SUPABASE_SERVICE_ROLE_KEY?: string;
}
