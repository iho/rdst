from __future__ import annotations

import json
import os
from typing import Any, Dict, Generator

import requests

from .base import LLMError, Provider, ProviderRequest, ProviderResponse


class OllamaProvider(Provider):
    """
    Ollama local LLM provider (OpenAI-compatible API).

    By default connects to http://localhost:11434
    Override with OLLAMA_HOST environment variable or config base_url.

    Model selection priority:
      1. RDST_OLLAMA_MODEL environment variable
      2. config.toml [llm] model
      3. First available model from Ollama API
      4. "llama3.2" (fallback)
    """

    _DEFAULT_HOST = "http://localhost:11434"

    def _get_host(self) -> str:
        """Return the base host (no path), e.g. http://localhost:11434."""
        env_host = os.getenv("OLLAMA_HOST")
        if env_host:
            return env_host.rstrip("/")

        try:
            from ..cli.rdst_cli import TargetsConfig
            config = TargetsConfig()
            config.load()
            base_url = config.get_llm_base_url()
            if base_url:
                # strip /v1/chat/completions if user stored the full path
                host = base_url.replace("/v1/chat/completions", "").rstrip("/")
                return host
        except Exception:
            pass

        return self._DEFAULT_HOST

    def _chat_url(self) -> str:
        return f"{self._get_host()}/v1/chat/completions"

    def _models_url(self) -> str:
        return f"{self._get_host()}/api/tags"

    def default_model(self) -> str:
        env_model = os.getenv("RDST_OLLAMA_MODEL")
        if env_model:
            return env_model
        return self._detect_first_model() or "llama3.2"

    def _detect_first_model(self) -> str | None:
        """Return the name of the first available Ollama model, or None."""
        try:
            resp = requests.get(self._models_url(), timeout=5)
            if resp.status_code == 200:
                models = resp.json().get("models", [])
                if models:
                    return models[0]["name"]
        except Exception:
            pass
        return None

    def list_models(self) -> list[str]:
        """Return names of all locally available Ollama models."""
        try:
            resp = requests.get(self._models_url(), timeout=5)
            if resp.status_code == 200:
                return [m["name"] for m in resp.json().get("models", [])]
        except Exception:
            pass
        return []

    def complete(
        self,
        request: ProviderRequest,
        *,
        api_key: str = "not-needed",
        base_url: str | None = None,
        extra_headers: dict | None = None,
        debug: bool = False,
    ) -> ProviderResponse:
        headers = {"Content-Type": "application/json"}
        if api_key and api_key not in ("not-needed", ""):
            headers["Authorization"] = f"Bearer {api_key}"

        payload: Dict[str, Any] = {
            "model": request.model,
            "messages": request.messages,
            "temperature": request.temperature,
            "stream": False,
        }
        if request.max_tokens is not None:
            payload["max_tokens"] = request.max_tokens
        if request.top_p is not None:
            payload["top_p"] = request.top_p
        if request.stop_sequences:
            payload["stop"] = list(request.stop_sequences)

        payload.update(request.extra or {})

        chat_url = base_url or self._chat_url()
        try:
            resp = requests.post(
                chat_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=300,
            )
        except requests.exceptions.ConnectionError as e:
            raise LLMError(
                f"Ollama connection error: {e}. Is Ollama running at {self._get_host()}?\n"
                "Start it with: ollama serve",
                code="CONNECTION_ERROR",
                cause=e,
            )
        except Exception as e:
            raise LLMError(f"Ollama request error: {e}", code="HTTP_ERROR", cause=e)

        if resp.status_code >= 400:
            try:
                err_json = resp.json()
            except Exception:
                err_json = {"error": {"message": resp.text}}
            msg = err_json.get("error", {}).get("message", f"HTTP {resp.status_code}")
            raise LLMError(f"Ollama error: {msg}", code="PROVIDER_HTTP", status=resp.status_code)

        data = resp.json()
        try:
            choice = data["choices"][0]
            text = choice.get("message", {}).get("content", "") or ""
            usage = data.get("usage", {}) or {}
            out_usage = {
                "prompt_tokens": usage.get("prompt_tokens"),
                "completion_tokens": usage.get("completion_tokens"),
                "total_tokens": usage.get("total_tokens"),
            }
        except Exception as e:
            raise LLMError(f"Ollama response parse error: {e}", code="PARSE_ERROR", cause=e)

        return ProviderResponse(text=text, usage=out_usage, raw={"raw": data} if debug else {})

    def stream(
        self,
        request: ProviderRequest,
        *,
        api_key: str = "not-needed",
        base_url: str | None = None,
        extra_headers: dict | None = None,
    ) -> Generator[str, None, None]:
        headers = {"Content-Type": "application/json"}
        if api_key and api_key not in ("not-needed", ""):
            headers["Authorization"] = f"Bearer {api_key}"

        payload: Dict[str, Any] = {
            "model": request.model,
            "messages": request.messages,
            "temperature": request.temperature,
            "stream": True,
        }
        if request.max_tokens is not None:
            payload["max_tokens"] = request.max_tokens
        if request.top_p is not None:
            payload["top_p"] = request.top_p
        if request.stop_sequences:
            payload["stop"] = list(request.stop_sequences)

        payload.update(request.extra or {})

        chat_url = base_url or self._chat_url()
        try:
            resp = requests.post(
                chat_url,
                headers=headers,
                data=json.dumps(payload),
                stream=True,
                timeout=300,
            )
        except requests.exceptions.ConnectionError as e:
            raise LLMError(
                f"Ollama connection error: {e}. Is Ollama running at {self._get_host()}?\n"
                "Start it with: ollama serve",
                code="CONNECTION_ERROR",
                cause=e,
            )

        if resp.status_code >= 400:
            raise LLMError(
                f"Ollama streaming error: HTTP {resp.status_code}",
                code="PROVIDER_HTTP",
                status=resp.status_code,
            )

        for line in resp.iter_lines():
            if not line:
                continue
            text = line.decode("utf-8") if isinstance(line, bytes) else line
            if text.startswith("data: "):
                text = text[6:]
            if text.strip() == "[DONE]":
                break
            try:
                chunk = json.loads(text)
                delta = chunk.get("choices", [{}])[0].get("delta", {})
                token = delta.get("content", "")
                if token:
                    yield token
            except (json.JSONDecodeError, IndexError, KeyError):
                continue
