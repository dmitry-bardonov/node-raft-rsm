import type { ClusterSnapshot, SimulationAction } from '../shared/protocol.js';

export async function fetchSnapshot(): Promise<ClusterSnapshot> {
  const response = await fetch('/api/snapshot');
  return parseResponse(response);
}

export async function sendAction(action: SimulationAction): Promise<ClusterSnapshot> {
  const response = await fetch('/api/action', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(action),
  });
  return parseResponse(response);
}

async function parseResponse(response: Response): Promise<ClusterSnapshot> {
  const body = (await response.json()) as ClusterSnapshot | { readonly error: string };
  if (!response.ok)
    throw new Error(
      'error' in body ? body.error : `request failed (${response.status.toString()})`,
    );
  return body as ClusterSnapshot;
}
