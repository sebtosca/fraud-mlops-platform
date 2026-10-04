# ADR-0004: Self-host the case-summary LLM on vLLM rather than call a hosted API

Status: accepted (2026-10-02, discovery Q28)

## Context
The target role asks for experience deploying LLMs and the LLMOps ecosystem. A hosted API (Claude, OpenAI) would be simpler and higher quality but demonstrates integration, not deployment. The user has an RTX 3090 (24 GB).

## Decision
Serve a ~7–8B quantised open-weight model with vLLM (OpenAI-compatible API) on the local GPU. A separate case-summary microservice calls it. Prompts are versioned in the MLflow prompt registry; an eval suite (PromptGuard's approach) gates prompt or model changes in CI on a self-hosted GPU runner. Token, latency and cost metrics are recorded.

## Consequences
- Demonstrates model serving, GPU scheduling, prompt versioning and eval gating end to end.
- Summary quality is below frontier hosted models; acceptable for an analyst aid in a demo.
- CI eval needs the self-hosted runner on the user's machine; gitlab.com shared runners cannot run it.
- Because the service speaks the OpenAI-compatible API, swapping to a hosted model is a config change.
