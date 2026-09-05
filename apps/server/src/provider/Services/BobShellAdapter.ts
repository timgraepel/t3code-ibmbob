/**
 * BobShellAdapter — service shape type for the Bob Shell provider adapter.
 *
 * @module provider/Services/BobShellAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * BobShellAdapterShape — per-instance Bob Shell adapter contract.
 */
export interface BobShellAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
