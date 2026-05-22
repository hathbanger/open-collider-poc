import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { serveStatic } from '@hono/node-server/serve-static';
import { streamSSE } from 'hono/streaming';
import { readFileSync } from 'fs';

const app = new Hono();

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const MODEL = 'anthropic/claude-sonnet-4.5';
const BASE_URL = 'https://openrouter.ai/api/v1/chat/completions';

async function llmCall(messages, { stream = false } = {}) {
  const res = await fetch(BASE_URL, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
      'Content-Type': 'application/json',
      'HTTP-Referer': 'https://open-collider-poc.fly.dev',
      'X-Title': 'Open Collider POC',
    },
    body: JSON.stringify({
      model: MODEL,
      messages,
      stream,
      temperature: 1,
      max_tokens: 4096,
    }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`OpenRouter error ${res.status}: ${text}`);
  }
  return res;
}

function extractJSON(text) {
  // Try to find JSON array in the response
  const match = text.match(/\[[\s\S]*\]/);
  if (match) return JSON.parse(match[0]);
  throw new Error('No JSON array found in response');
}

app.post('/api/collide', async (c) => {
  const { brief } = await c.req.json();
  if (!brief || !brief.trim()) return c.json({ error: 'Brief is required' }, 400);

  return streamSSE(c, async (stream) => {
    try {
      // Phase 1: Generate domains
      await stream.writeSSE({ data: JSON.stringify({ type: 'status', message: 'Generating distant domains...' }), event: 'message' });

      const domainPrompt = `You are a bisociation engine. Given an ideation brief, generate 4 structurally distant knowledge domains that have NOTHING obvious to do with the brief's field. Each domain must include:
1. Domain name (e.g. "fungal mycelium networks", "Ottoman tax farming", "semiconductor doping")
2. An active principle — a specific counter-intuitive mechanism from that domain
3. A bridging question — how might this principle apply to the brief?

Rules:
- Domains must be from DIFFERENT fields (biology, physics, history, economics, manufacturing, etc.)
- The more counter-intuitive the connection, the better
- Avoid cliché metaphors (no "ecosystem", no "DNA of the company")
- Each active principle must be a real, specific mechanism — not a vague analogy

Brief: ${brief}

Return ONLY a JSON array: [{"domain": "...", "active_principle": "...", "bridging_question": "..."}]`;

      const domainRes = await llmCall([{ role: 'user', content: domainPrompt }]);
      const domainData = await domainRes.json();
      const domainText = domainData.choices[0].message.content;
      const domains = extractJSON(domainText);

      await stream.writeSSE({ data: JSON.stringify({ type: 'domains', domains }), event: 'message' });

      // Phase 2: Collide each domain
      for (let i = 0; i < domains.length; i++) {
        const d = domains[i];
        await stream.writeSSE({ data: JSON.stringify({ type: 'status', message: `Colliding: ${d.domain}...` }), event: 'message' });

        const collisionPrompt = `You are generating ideas through bisociation — colliding a distant domain with a problem brief.

Brief: ${brief}
Distant Domain: ${d.domain}
Active Principle: ${d.active_principle}
Bridging Question: ${d.bridging_question}

Generate 2 non-trivial ideas that could ONLY exist because of this collision. Each idea must:
1. Have a concrete name
2. Explain the mechanism (how the active principle transfers)
3. Be actionable, not metaphorical
4. Include "↳ from ${d.domain}" attribution

Return ONLY a JSON array: [{"name": "...", "mechanism": "...", "attribution": "..."}]`;

        const collisionRes = await llmCall([{ role: 'user', content: collisionPrompt }]);
        const collisionData = await collisionRes.json();
        const collisionText = collisionData.choices[0].message.content;
        const ideas = extractJSON(collisionText);

        await stream.writeSSE({
          data: JSON.stringify({ type: 'collision', domainIndex: i, domain: d.domain, ideas }),
          event: 'message',
        });
      }

      await stream.writeSSE({ data: JSON.stringify({ type: 'done' }), event: 'message' });
    } catch (err) {
      console.error('Collide error:', err);
      await stream.writeSSE({ data: JSON.stringify({ type: 'error', message: err.message }), event: 'message' });
    }
  });
});

app.post('/api/regen-examples', async (c) => {
  try {
    const { motif } = await c.req.json().catch(() => ({}));
    const motifClause = motif
      ? `\n\nMOTIF CONSTRAINT: Every brief must be colored by the motif "${motif}". The motif is a thematic lens — it shapes the *energy* and *angle* of the briefs, not their literal subject. Don't mention the motif word directly in the briefs. Let it seep into the framing, the tension, the way the problem is posed.`
      : '';
    const res = await llmCall([{
      role: 'user',
      content: `Generate 20 provocative, diverse ideation briefs for a bisociation/creativity engine. Each brief should be a single sentence — a real problem worth solving or a design challenge worth exploring.

Rules:
- Cover wildly different domains: tech, health, education, cities, food, finance, culture, science, politics, art, sports, relationships, work, environment, etc.
- Mix scales: personal habits, product design, systemic change, policy, physical spaces
- Make them punchy and specific — not vague or generic
- Some should be contrarian or uncomfortable
- Some should be playful/fun
- All should make someone think "ooh I want to see what the collider does with THAT"${motifClause}

Return ONLY a JSON array of 20 strings.`
    }], { stream: false });
    const data = await res.json();
    const text = data.choices[0].message.content;
    const briefs = extractJSON(text);
    return c.json({ briefs });
  } catch (err) {
    return c.json({ error: err.message }, 500);
  }
});

// Serve static files
app.use('/*', serveStatic({ root: './public' }));

const port = 8080;
serve({ fetch: app.fetch, port }, () => {
  console.log(`Open Collider POC running on http://localhost:${port}`);
});
