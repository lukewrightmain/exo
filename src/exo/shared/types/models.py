from typing import Optional

from pydantic import PositiveInt

from exo.shared.types.common import Id
from exo.shared.types.memory import Memory
from exo.utils.pydantic_ext import CamelCaseModel


class ModelId(Id):
    pass


class ModelMetadata(CamelCaseModel):
    model_id: ModelId
    pretty_name: str
    storage_size: Memory
    n_layers: PositiveInt

    # Runtime memory estimation fields
    # During model loading, memory usage spikes above final loaded size
    loading_overhead_factor: float = 1.10  # 10% overhead during load

    # Reserve memory for Android Low Memory Killer to prevent OOM kills
    android_safety_margin: float = 0.10  # Reserve 10% for Android system

    # KV cache memory per token (estimated from model config)
    # For large models: typically 256-512 bytes per token per layer
    kv_cache_per_token_bytes: int = 0

    # Preferred GGUF quantization (e.g., "Q4_K_M", "Q3_K_M")
    preferred_gguf_quant: Optional[str] = None

    def get_runtime_memory(self, context_window: int = 1024) -> Memory:
        """
        Calculate actual runtime memory required for inference.

        This is more accurate than storage_size because it accounts for:
        1. Loading overhead (memory spikes during model load)
        2. KV cache for context window
        3. Safety margin for system stability

        Args:
            context_window: Maximum context length for KV cache calculation

        Returns:
            Memory object representing required runtime memory
        """
        base = self.storage_size.in_bytes

        # Add loading overhead
        loading = base * self.loading_overhead_factor

        # Add KV cache estimate
        kv_cache = self.kv_cache_per_token_bytes * context_window * self.n_layers

        return Memory.from_bytes(int(loading + kv_cache))

    def get_safe_memory_requirement(self, context_window: int = 1024) -> Memory:
        """
        Calculate memory requirement with Android safety margin.

        Use this for placement decisions to ensure nodes have enough
        headroom to avoid OOM kills from Android's Low Memory Killer.

        Args:
            context_window: Maximum context length for KV cache calculation

        Returns:
            Memory object with safety margin applied
        """
        runtime = self.get_runtime_memory(context_window)

        # Add Android safety margin
        # If model needs X bytes, we need X / (1 - margin) bytes available
        if self.android_safety_margin > 0:
            safe_bytes = int(runtime.in_bytes / (1 - self.android_safety_margin))
            return Memory.from_bytes(safe_bytes)

        return runtime
