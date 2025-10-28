# utils/ai_narrator.py
import os, httpx, json, time
from typing import List

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Allow overriding model (single) or a CSV list for fallbacks
# Example: OPENROUTER_MODELS="deepseek/deepseek-chat-v3.1:free,qwen/qwen-2.5-7b-instruct:free"
_DEFAULT_MODELS = [
    "deepseek/deepseek-r1-0528:free",
    "mistralai/mistral-small-3.2-24b-instruct:free",
    "openai/gpt-oss-20b:free"
]


def _resolve_models() -> List[str]:
    # Highest priority: explicit single model var
    single = os.getenv("OPENROUTER_MODEL")
    if single:
        return [single]
    # Next: CSV list of models for fallback
    multi = os.getenv("OPENROUTER_MODELS")
    if multi:
        return [m.strip() for m in multi.split(",") if m.strip()]
    # Default list
    return _DEFAULT_MODELS


def _post_openrouter(model: str, prompt: str, timeout: int = 60) -> str:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENROUTER_API_KEY in environment/secrets.")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        # Optional attribution headers (recommended by OpenRouter):
        "HTTP-Referer": os.environ.get("APP_URL", "https://streamlit.io"),
        "X-Title": os.environ.get("APP_NAME", "Talent Match Dashboard"),
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "user", "content": prompt}
        ]
    }

    with httpx.Client(timeout=timeout) as client:
        r = client.post(OPENROUTER_URL, headers=headers, content=json.dumps(payload))

    # Parse JSON or throw with raw body for debugging
    try:
        data = r.json()
    except Exception:
        snippet = r.text[:500] if hasattr(r, "text") else str(r.content)[:500]
        raise RuntimeError(f"OpenRouter returned non-JSON (status {r.status_code}): {snippet}")

    if r.status_code >= 400:
        err = data.get("error") if isinstance(data, dict) else data
        raise RuntimeError(f"OpenRouter error {r.status_code}: {err}")

    try:
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        raise RuntimeError(f"Unexpected OpenRouter response shape: {data}") from e


def _call_with_retry(models: List[str], prompt: str, max_retries: int = 4) -> str:
    """Try models in order. For 429/5xx errors, retry with exponential backoff and/or next model."""
    last_err = None
    for model in models:
        backoff = 1.0
        for attempt in range(max_retries):
            try:
                return _post_openrouter(model, prompt)
            except RuntimeError as e:
                msg = str(e)
                last_err = e
                # Prefer switching model immediately on 429 (rate-limited upstream)
                if "error 429" in msg or "rate-limit" in msg.lower() or "temporarily rate-limited" in msg.lower():
                    # move to next model without waiting too long
                    break
                # For transient 5xx or gateway-ish issues, backoff
                if any(x in msg for x in [" 502", " 503", " 504", "gateway", "timeout"]):
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                # Other errors: no retry on this model
                break
        # try next model
        continue
    # If all models failed
    raise last_err if last_err else RuntimeError("All OpenRouter model attempts failed.")


def generate_job_profile(role, level, purpose):
    prompt = f"""You are an HR analyst. Create three sections for a job profile:
1) Job requirements (bullet points),
2) Job description (1 short paragraph),
3) Key competencies (bullet points).
Role: {role}; Level: {level}; Purpose: {purpose}.
Keep it concise, business-ready, bias-aware, and tailored to analytics roles."""

    models = _resolve_models()
    return _call_with_retry(models, prompt)