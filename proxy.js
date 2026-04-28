// api/v1/[...path].js
// Vercel Serverless Function — proxy Fireworks.ai
// Deploy grátis em: vercel.com

const FIREWORKS_BASE = "https://api.fireworks.ai/inference/v1";

const MODEL_MAP = {
  "deepseek-v3":      "accounts/fireworks/models/deepseek-v3p2",
  "glm-5.1":          "accounts/fireworks/models/glm-5p1",
  "kimi-k2.5":        "accounts/fireworks/models/kimi-k2p5",
  "qwen3.6":          "accounts/fireworks/models/qwen3p6-plus",
  "llama-4-maverick": "accounts/fireworks/models/llama4-maverick-instruct-basic",
  "llama-4-scout":    "accounts/fireworks/models/llama4-scout-instruct-basic",
  "llama-3.3-70b":    "accounts/fireworks/models/llama-v3p3-70b-instruct",
  "mixtral-8x22b":    "accounts/fireworks/models/mixtral-8x22b-instruct",
};

function resolveModel(model = "") {
  if (model.startsWith("accounts/")) return model;
  if (MODEL_MAP[model]) return MODEL_MAP[model];
  return `accounts/fireworks/models/${model}`;
}

function setCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "*");
}

export default async function handler(req, res) {
  setCors(res);

  if (req.method === "OPTIONS") {
    return res.status(204).end();
  }

  const path = req.query.path || [];
  const route = Array.isArray(path) ? path.join("/") : path;

  // Health check
  if (route === "" || route === "health") {
    return res.status(200).json({
      status: "ok",
      service: "fireworks-proxy-vercel",
      endpoint: "/api/v1/chat/completions",
      models: "/api/v1/models",
    });
  }

  // Lista de modelos
  if (route === "models" && req.method === "GET") {
    const models = [
      ...Object.values(MODEL_MAP),
      ...Object.keys(MODEL_MAP),
    ].map((id) => ({ id, object: "model", created: 1717200000, owned_by: "fireworks" }));
    return res.status(200).json({ object: "list", data: models });
  }

  // Chat completions
  if (route === "chat/completions" && req.method === "POST") {
    // Pega API key do header
    const auth = req.headers["authorization"] || "";
    const xKey = req.headers["x-api-key"] || "";
    const apiKey = process.env.FIREWORKS_API_KEY ||
      auth.replace(/^Bearer\s+/i, "") ||
      xKey;

    if (!apiKey) {
      return res.status(401).json({
        error: { message: "API key não encontrada. Coloque no chub.ai ou configure FIREWORKS_API_KEY no Vercel.", type: "auth_error" }
      });
    }

    const body = req.body;
    if (!body) {
      return res.status(400).json({ error: { message: "Body vazio", type: "invalid_request" } });
    }

    body.model = resolveModel(body.model);
    const isStream = body.stream === true;

    const fwRes = await fetch(`${FIREWORKS_BASE}/chat/completions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`,
      },
      body: JSON.stringify(body),
    });

    if (!fwRes.ok) {
      const err = await fwRes.text();
      return res.status(fwRes.status).send(err);
    }

    // Streaming
    if (isStream) {
      res.setHeader("Content-Type", "text/event-stream");
      res.setHeader("Cache-Control", "no-cache");
      res.setHeader("Connection", "keep-alive");

      const reader = fwRes.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          res.write("data: [DONE]\n\n");
          return res.end();
        }

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop();

        for (const line of lines) {
          if (!line.startsWith("data: ") || line === "data: [DONE]") {
            res.write(line + "\n");
            continue;
          }
          try {
            const json = JSON.parse(line.slice(6));
            // Remove reasoning_content que quebra o chub.ai
            if (json.choices) {
              for (const choice of json.choices) {
                if (choice.delta) {
                  delete choice.delta.reasoning_content;
                  const keys = Object.keys(choice.delta).filter(
                    k => choice.delta[k] != null && choice.delta[k] !== ""
                  );
                  if (keys.length === 0 && choice.finish_reason == null) continue;
                }
              }
            }
            res.write("data: " + JSON.stringify(json) + "\n");
          } catch {
            res.write(line + "\n");
          }
        }
      }
    }

    // Resposta normal
    const data = await fwRes.json();
    if (data.choices) {
      for (const choice of data.choices) {
        if (choice.message) {
          delete choice.message.reasoning_content;
          delete choice.message.annotations;
          delete choice.message.audio;
          if (!choice.message.tool_calls?.length) delete choice.message.tool_calls;
        }
      }
    }
    return res.status(200).json(data);
  }

  return res.status(404).json({ error: { message: `Rota não encontrada: ${route}` } });
}
