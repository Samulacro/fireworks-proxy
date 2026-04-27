/**
 * Fireworks.ai Proxy Worker
 *
 * Recebe requests no formato OpenAI (ex: Chub.ai) e
 * reencaminha para a API do Fireworks.ai, retornando
 * a resposta no mesmo formato OpenAI-compatível.
 */

// Mapeamento de modelos: nome_que_o_cliente_envia -> nome_que_o_fireworks_espera
const MODEL_MAP = {
  // DeepSeek
  'deepseek-v3':  'accounts/fireworks/models/deepseek-v3p2',
  'deepseek-v3.1':'accounts/fireworks/models/deepseek-v3p2',
  // GLM
  'glm-5.1':      'accounts/fireworks/models/glm-5p1',
  // Kimi
  'kimi-k2.5':    'accounts/fireworks/models/kimi-k2p5',
  // Qwen
  'qwen3.6':      'accounts/fireworks/models/qwen3p6-plus',
  'qwen3-235b':   'accounts/fireworks/models/qwen3-235b-a22b-instruct-2507',
  'qwen3-coder':  'accounts/fireworks/models/qwen3-coder',
  // Llama 4
  'llama-4-maverick': 'accounts/fireworks/models/llama4-maverick-instruct-basic',
  'llama-4-scout':    'accounts/fireworks/models/llama4-scout-instruct-basic',
  // Mixtral
  'mixtral-8x22b':   'accounts/fireworks/models/mixtral-8x22b-instruct',
  // Fallback
  'default':      'accounts/fireworks/models/deepseek-v3p2',
};

// Headers que devem ser repassados do cliente ao Fireworks
const FORWARD_HEADERS = [
  'content-type',
  'accept',
  'x-request-id',
];

// Headers que NÃO devem ser repassados ao cliente na resposta
const STRIP_RESPONSE_HEADERS = new Set([
  'cf-ray',
  'report-to',
  'nel',
  'server',
  'set-cookie',
]);

function mapModel(clientModel) {
  // Verifica match exato
  if (MODEL_MAP[clientModel]) {
    return MODEL_MAP[clientModel];
  }
  // Verifica match parcial (ignora sufixos como datas/versões)
  for (const [key, value] of Object.entries(MODEL_MAP)) {
    if (clientModel.startsWith(key)) {
      return value;
    }
  }
  // Fallback: usa o modelo como está (pode ser um ID direto do Fireworks)
  console.log(`[proxy] modelo "${clientModel}" não mapeado, usando como está`);
  return clientModel || MODEL_MAP['default'];
}

function buildFireworksHeaders(request, apiKey) {
  const headers = new Headers();
  headers.set('Authorization', `Bearer ${apiKey}`);
  headers.set('Content-Type', 'application/json');

  // Repassa headers úteis do cliente
  for (const h of FORWARD_HEADERS) {
    const val = request.headers.get(h);
    if (val) headers.set(h, val);
  }

  return headers;
}

function parseApiKey(request, env) {
  // 1. Tenta Authorization header do cliente (Bearer token do Fireworks direto)
  const auth = request.headers.get('Authorization');
  if (auth && auth.startsWith('Bearer ')) {
    const token = auth.slice(7);
    // Se for uma API key do Fireworks (começa com "fw_" ou é longa),
    // usa diretamente
    if (token.startsWith('fw_') || token.length > 40) {
      return token;
    }
  }

  // 2. Tenta header x-api-key
  const xApiKey = request.headers.get('x-api-key');
  if (xApiKey) return xApiKey;

  // 3. Tenta variável de ambiente do Worker
  if (env && env.FIREWORKS_API_KEY) return env.FIREWORKS_API_KEY;

  return null;
}

async function handleOptions(request) {
  const headers = new Headers();
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  headers.set('Access-Control-Allow-Headers', '*');
  headers.set('Access-Control-Max-Age', '86400');
  return new Response(null, { status: 204, headers });
}

function buildCorsHeaders(origin) {
  const headers = new Headers();
  headers.set('Access-Control-Allow-Origin', origin || '*');
  headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  headers.set('Access-Control-Allow-Headers', '*');
  headers.set('Access-Control-Expose-Headers', '*');
  return headers;
}

function cleanChunk(json) {
  // Remove reasoning_content dos deltas (modelos com thinking)
  // Se o delta ficar vazio, retorna null para pular o chunk
  if (!json.choices) return json;

  const cleaned = [];
  for (const choice of json.choices) {
    if (choice.delta) {
      delete choice.delta.reasoning_content;
      // Pula este choice se o delta ficou vazio e não é um finish
      const keys = Object.keys(choice.delta);
      if (keys.length === 0 && !choice.finish_reason) continue;
    }
    cleaned.push(choice);
  }

  // Se todas as choices foram removidas, pula o chunk inteiro
  if (cleaned.length === 0) return null;

  json.choices = cleaned;
  return json;
}

async function proxyStream(fireworksResponse, corsHeaders) {
  const reader = fireworksResponse.body.getReader();
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  const stream = new ReadableStream({
    async start(controller) {
      try {
        const enqueue = (s) => controller.enqueue(encoder.encode(s));

        let buffer = '';
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            if (buffer) enqueue(buffer + '\n');
            enqueue('data: [DONE]\n\n');
            controller.close();
            return;
          }

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop(); // última linha pode estar incompleta

          for (const line of lines) {
            if (line === '') {
              enqueue('\n');
              continue;
            }
            if (!line.startsWith('data: ')) {
              enqueue(line + '\n');
              continue;
            }
            if (line === 'data: [DONE]') {
              enqueue('data: [DONE]\n\n');
              continue;
            }

            try {
              const json = JSON.parse(line.slice(6));
              const cleaned = cleanChunk(json);
              if (cleaned === null) continue; // chunk era só thinking, pula
              enqueue('data: ' + JSON.stringify(cleaned) + '\n');
            } catch {
              enqueue(line + '\n');
            }
          }
        }
      } catch (err) {
        console.error('[proxy] erro no stream:', err.message);
        controller.error(err);
      }
    },
  });

  corsHeaders.set('Content-Type', 'text/event-stream');
  corsHeaders.set('Cache-Control', 'no-cache');
  corsHeaders.set('Connection', 'keep-alive');
  return new Response(stream, {
    status: fireworksResponse.status,
    headers: corsHeaders,
  });
}

async function proxyNonStream(fireworksResponse, corsHeaders) {
  let body;
  try {
    const json = await fireworksResponse.json();
    const cleaned = cleanChunk(json);
    body = JSON.stringify(cleaned || json);
  } catch {
    body = await fireworksResponse.text();
  }
  corsHeaders.set('Content-Type', 'application/json');
  return new Response(body, {
    status: fireworksResponse.status,
    headers: corsHeaders,
  });
}

async function handleChatCompletions(request, env, url) {
  const corsHeaders = buildCorsHeaders(request.headers.get('Origin'));

  // Parse API key
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

  // Parse body
  let body;
  try {
    body = await request.json();
  } catch {
    corsHeaders.set('Content-Type', 'application/json');
    return new Response(JSON.stringify({
      error: { message: 'Corpo da requisição inválido (JSON esperado)', type: 'invalid_request' }
    }), { status: 400, headers: corsHeaders });
  }

  // Mapeia modelo
  const originalModel = body.model;
  body.model = mapModel(originalModel);

  // Log
  console.log(`[proxy] modelo: "${originalModel}" -> "${body.model}"` +
    (body.stream ? ' [stream]' : ''));

  // Constrói request pro Fireworks
  const fireworksUrl = 'https://api.fireworks.ai/inference/v1/chat/completions';
  const fireworksHeaders = buildFireworksHeaders(request, apiKey);

  // Garante que stream seja compatível
  const isStream = body.stream === true;
  body.stream = isStream;

  // Envia request
  let fireworksResponse;
  try {
    fireworksResponse = await fetch(fireworksUrl, {
      method: 'POST',
      headers: fireworksHeaders,
      body: JSON.stringify(body),
    });
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

  // Se o Fireworks retornou erro, repassa como JSON
  if (!fireworksResponse.ok) {
    const errorBody = await fireworksResponse.text();
    corsHeaders.set('Content-Type', 'application/json');
    return new Response(errorBody, {
      status: fireworksResponse.status,
      headers: corsHeaders,
    });
  }

  // Retorna stream ou não-stream
  if (isStream) {
    return proxyStream(fireworksResponse, corsHeaders);
  } else {
    return proxyNonStream(fireworksResponse, corsHeaders);
  }
}

async function handleModels(request, env) {
  // Retorna lista estática de modelos disponíveis
  const corsHeaders = buildCorsHeaders(request.headers.get('Origin'));
  corsHeaders.set('Content-Type', 'application/json');

  const models = Object.entries(MODEL_MAP)
    .filter(([k]) => k !== 'default')
    .map(([id, _fireworksId]) => ({
      id,
      object: 'model',
      created: 1717200000,
      owned_by: 'fireworks-proxy',
    }));

  return new Response(JSON.stringify({
    object: 'list',
    data: models,
  }), { headers: corsHeaders });
}

// Roteador principal
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return handleOptions(request);
    }

    // Health check
    if (url.pathname === '/' || url.pathname === '/health') {
      return new Response(JSON.stringify({
        status: 'ok',
        service: 'fireworks-proxy',
        version: '1.0.0',
        endpoint: `${url.origin}/v1/chat/completions`,
        models: `${url.origin}/v1/models`,
      }), {
        headers: { 'Content-Type': 'application/json', ...Object.fromEntries(buildCorsHeaders('*')) }
      });
    }

    // GET /v1/models
    if (url.pathname === '/v1/models' && request.method === 'GET') {
      return handleModels(request, env);
    }

    // POST /v1/chat/completions
    if (url.pathname === '/v1/chat/completions' && request.method === 'POST') {
      return handleChatCompletions(request, env, url);
    }

    // Também aceita caminhos sem /v1 (alguns clientes usam assim)
    if (url.pathname === '/chat/completions' && request.method === 'POST') {
      return handleChatCompletions(request, env, url);
    }

    // 404
    const corsHeaders = buildCorsHeaders(request.headers.get('Origin'));
    corsHeaders.set('Content-Type', 'application/json');
    return new Response(JSON.stringify({
      error: { message: `Rota não encontrada: ${url.pathname}`, type: 'not_found' }
    }), { status: 404, headers: corsHeaders });
  },
};
