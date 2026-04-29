/**
 * Fireworks.ai Proxy Worker
 *
 * Recebe requests no formato OpenAI (ex: Chub.ai) e
 * reencaminha para a API do Fireworks.ai, retornando
 * a resposta no mesmo formato OpenAI-compatível.
 *
 * Não altera o payload — reasoning_content, thinking blocks etc
 * passam transparentes. O Fireworks já retorna OpenAI-compatível.
 */

// Mapeamento de modelos: nome_que_o_cliente_envia -> nome_que_o_fireworks_espera
const MODEL_MAP = {
  // DeepSeek
  'deepseek-v3':      'accounts/fireworks/models/deepseek-v3p2',
  'deepseek-v3.1':    'accounts/fireworks/models/deepseek-v3p2',
  'deepseek-v4-pro':  'accounts/fireworks/models/deepseek-v4-pro',
  // GLM
  'glm-5':            'accounts/fireworks/models/glm-5',
  'glm-5.1':          'accounts/fireworks/models/glm-5p1',
  // Kimi
  'kimi-k2.5':        'accounts/fireworks/models/kimi-k2p5',
  'kimi-k2.6':        'accounts/fireworks/models/kimi-k2p6',
  // MiniMax
  'minimax-m2.7':     'accounts/fireworks/models/minimax-m2p7',
  // Qwen
  'qwen3.6':          'accounts/fireworks/models/qwen3p6-plus',
  'qwen3-235b':       'accounts/fireworks/models/qwen3-235b-a22b-instruct-2507',
  'qwen3-coder':      'accounts/fireworks/models/qwen3-coder',
  // Llama 4
  'llama-4-maverick': 'accounts/fireworks/models/llama4-maverick-instruct-basic',
  'llama-4-scout':    'accounts/fireworks/models/llama4-scout-instruct-basic',
  // Mixtral
  'mixtral-8x22b':    'accounts/fireworks/models/mixtral-8x22b-instruct',
  // Fallback
  'default':          'accounts/fireworks/models/deepseek-v3p2',
};

const FORWARD_HEADERS = [
  'content-type',
  'accept',
  'x-request-id',
];

const STRIP_RESPONSE_HEADERS = new Set([
  'cf-ray',
  'report-to',
  'nel',
  'server',
  'set-cookie',
]);

function mapModel(clientModel) {
  if (MODEL_MAP[clientModel]) return MODEL_MAP[clientModel];
  for (const [key, value] of Object.entries(MODEL_MAP)) {
    if (clientModel.startsWith(key)) return value;
  }
  console.log(`[proxy] modelo "${clientModel}" não mapeado, usando como está`);
  return clientModel || MODEL_MAP['default'];
}

function buildFireworksHeaders(request, apiKey) {
  const headers = new Headers();
  headers.set('Authorization', `Bearer ${apiKey}`);
  headers.set('Content-Type', 'application/json');
  for (const h of FORWARD_HEADERS) {
    const val = request.headers.get(h);
    if (val) headers.set(h, val);
  }
  return headers;
}

function parseApiKey(request, env) {
  const auth = request.headers.get('Authorization');
  if (auth && auth.startsWith('Bearer ')) {
    const token = auth.slice(7);
    if (token.startsWith('fw_') || token.length > 40) return token;
  }
  const xApiKey = request.headers.get('x-api-key');
  if (xApiKey) return xApiKey;
  if (env && env.FIREWORKS_API_KEY) return env.FIREWORKS_API_KEY;
  return null;
}

function buildCorsHeaders(origin) {
  const headers = new Headers();
  headers.set('Access-Control-Allow-Origin', origin || '*');
  headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  headers.set('Access-Control-Allow-Headers', '*');
  headers.set('Access-Control-Expose-Headers', '*');
  return headers;
}

async function handleOptions() {
  const headers = new Headers();
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  headers.set('Access-Control-Allow-Headers', '*');
  headers.set('Access-Control-Max-Age', '86400');
  return new Response(null, { status: 204, headers });
}

// ── Streaming: repassa o body do Fireworks sem alterar nada ──────────────

async function proxyStream(fireworksResponse, corsHeaders) {
  corsHeaders.set('Content-Type', 'text/event-stream');
  corsHeaders.set('Cache-Control', 'no-cache');
  corsHeaders.set('Connection', 'keep-alive');
  return new Response(fireworksResponse.body, {
    status: fireworksResponse.status,
    headers: corsHeaders,
  });
}

// ── Não-streaming: repassa o JSON do Fireworks sem alterar nada ──────────

async function proxyNonStream(fireworksResponse, corsHeaders) {
  const body = await fireworksResponse.text();
  corsHeaders.set('Content-Type', 'application/json');
  return new Response(body, {
    status: fireworksResponse.status,
    headers: corsHeaders,
  });
}

// ── Handlers ─────────────────────────────────────────────────────────────

async function handleChatCompletions(request, env) {
  const corsHeaders = buildCorsHeaders(request.headers.get('Origin'));

  const apiKey = parseApiKey(request, env);
  if (!apiKey) {
    corsHeaders.set('Content-Type', 'application/json');
    return new Response(JSON.stringify({
      error: {
        message: 'API key do Fireworks.ai não encontrada. Passe no header Authorization: Bearer fw_... ou configure FIREWORKS_API_KEY no Worker.',
        type: 'auth_error',
      }
    }), { status: 401, headers: corsHeaders });
  }

  let body;
  try {
    body = await request.json();
  } catch {
    corsHeaders.set('Content-Type', 'application/json');
    return new Response(JSON.stringify({
      error: { message: 'Corpo da requisição inválido (JSON esperado)', type: 'invalid_request' }
    }), { status: 400, headers: corsHeaders });
  }

  const originalModel = body.model;
  body.model = mapModel(originalModel);
  console.log(`[proxy] modelo: "${originalModel}" -> "${body.model}"` +
    (body.stream ? ' [stream]' : ''));

  const isStream = body.stream === true;
  body.stream = isStream;

  let fireworksResponse;
  try {
    fireworksResponse = await fetch(
      'https://api.fireworks.ai/inference/v1/chat/completions',
      {
        method: 'POST',
        headers: buildFireworksHeaders(request, apiKey),
        body: JSON.stringify(body),
      }
    );
  } catch (err) {
    console.error('[proxy] erro fetch:', err.message);
    corsHeaders.set('Content-Type', 'application/json');
    return new Response(JSON.stringify({
      error: { message: `Erro ao conectar no Fireworks.ai: ${err.message}`, type: 'proxy_error' }
    }), { status: 502, headers: corsHeaders });
  }

  // Repassa headers de rate limit
  for (const [key, value] of fireworksResponse.headers.entries()) {
    if (!STRIP_RESPONSE_HEADERS.has(key.toLowerCase())) {
      corsHeaders.set(key, value);
    }
  }

  if (!fireworksResponse.ok) {
    const errorBody = await fireworksResponse.text();
    corsHeaders.set('Content-Type', 'application/json');
    return new Response(errorBody, {
      status: fireworksResponse.status,
      headers: corsHeaders,
    });
  }

  return isStream
    ? proxyStream(fireworksResponse, corsHeaders)
    : proxyNonStream(fireworksResponse, corsHeaders);
}

async function handleModels(request) {
  const corsHeaders = buildCorsHeaders(request.headers.get('Origin'));
  corsHeaders.set('Content-Type', 'application/json');

  const models = Object.entries(MODEL_MAP)
    .filter(([k]) => k !== 'default')
    .map(([id]) => ({
      id,
      object: 'model',
      created: 1717200000,
      owned_by: 'fireworks-proxy',
    }));

  return new Response(JSON.stringify({ object: 'list', data: models }), {
    headers: corsHeaders,
  });
}

// ── Roteador principal ───────────────────────────────────────────────────

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') return handleOptions();

    if (url.pathname === '/' || url.pathname === '/health') {
      return new Response(JSON.stringify({
        status: 'ok',
        service: 'fireworks-proxy',
        version: '2.0.0',
        endpoint: `${url.origin}/v1/chat/completions`,
        models: `${url.origin}/v1/models`,
      }), {
        headers: {
          'Content-Type': 'application/json',
          ...Object.fromEntries(buildCorsHeaders('*')),
        }
      });
    }

    if (url.pathname === '/v1/models' && request.method === 'GET') {
      return handleModels(request);
    }

    if ((url.pathname === '/v1/chat/completions' || url.pathname === '/chat/completions')
        && request.method === 'POST') {
      return handleChatCompletions(request, env);
    }

    const corsHeaders = buildCorsHeaders(request.headers.get('Origin'));
    corsHeaders.set('Content-Type', 'application/json');
    return new Response(JSON.stringify({
      error: { message: `Rota não encontrada: ${url.pathname}`, type: 'not_found' }
    }), { status: 404, headers: corsHeaders });
  },
};