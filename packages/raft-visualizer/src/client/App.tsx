import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from 'react';
import {
  Background,
  Controls,
  Handle,
  Position,
  ReactFlow,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type Edge,
  type Node,
  type NodeProps,
  type ReactFlowInstance,
} from '@xyflow/react';
import type { ClusterSnapshot, NodeSnapshot, SimulationAction } from '../shared/protocol.js';
import { fetchSnapshot, sendAction } from './api.js';

interface FlowPosition {
  readonly x: number;
  readonly y: number;
}

interface RaftNodeData extends Record<string, unknown> {
  readonly snapshot: NodeSnapshot;
  readonly selected: boolean;
  readonly onSelect: (nodeId: string) => void;
  readonly onMove: (nodeId: string, position: FlowPosition) => void;
}

type RaftFlowNode = Node<RaftNodeData, 'raft'>;

const nodeTypes = { raft: RaftNodeCard };
const initialEdges: Edge[] = [];

export function App() {
  const [snapshot, setSnapshot] = useState<ClusterSnapshot>();
  const [selected, setSelected] = useState('node-1');
  const [partition, setPartition] = useState<readonly string[]>([]);
  const [nodeCount, setNodeCount] = useState(3);
  const [seed, setSeed] = useState(1);
  const [key, setKey] = useState('counter');
  const [value, setValue] = useState('1');
  const [error, setError] = useState<string>();
  const [running, setRunning] = useState(false);
  const [speed, setSpeed] = useState(500);
  const [flow, setFlow] = useState<ReactFlowInstance<RaftFlowNode>>();
  const [flowNodes, setFlowNodes, onNodesChange] = useNodesState<RaftFlowNode>([]);
  const [flowEdges, setFlowEdges, onEdgesChange] = useEdgesState(initialEdges);
  const layoutKey = snapshot?.nodes.map((node) => `${node.id}:${node.role}`).join('|') ?? '';
  const selectFlowNode = useCallback((nodeId: string) => {
    setSelected(nodeId);
  }, []);
  const moveFlowNode = useCallback(
    (nodeId: string, position: FlowPosition) => {
      setFlowNodes((nodes) =>
        nodes.map((node) => (node.id === nodeId ? { ...node, position } : node)),
      );
    },
    [setFlowNodes],
  );
  const graph = useMemo(
    () => buildGraph(snapshot, selected, selectFlowNode, moveFlowNode),
    [moveFlowNode, selectFlowNode, selected, snapshot],
  );

  useEffect(() => {
    void fetchSnapshot()
      .then(setSnapshot)
      .catch((reason: unknown) => {
        setError(asError(reason).message);
      });
    const events = new EventSource('/api/events');
    events.onmessage = (event) => {
      setSnapshot(JSON.parse(event.data as string) as ClusterSnapshot);
      setError(undefined);
    };
    events.onerror = () => {
      setError('Live update connection interrupted; reconnecting…');
    };
    return () => {
      events.close();
    };
  }, []);

  const act = useCallback(async (action: SimulationAction) => {
    try {
      setSnapshot(await sendAction(action));
      setError(undefined);
    } catch (reason) {
      setError(asError(reason).message);
    }
  }, []);

  useEffect(() => {
    if (!running) return undefined;
    const timer = window.setInterval(() => void act({ type: 'step' }), speed);
    return () => {
      window.clearInterval(timer);
    };
  }, [act, running, speed]);

  useEffect(() => {
    setFlowNodes((current) =>
      graph.nodes.map((next) => {
        const existing = current.find((node) => node.id === next.id);
        return existing === undefined
          ? next
          : { ...existing, ...next, position: existing.position, data: next.data };
      }),
    );
    setFlowEdges(graph.edges);
  }, [graph, setFlowEdges, setFlowNodes]);

  useEffect(() => {
    if (flow === undefined || snapshot === undefined) return undefined;
    const frame = window.requestAnimationFrame(() => {
      void flow.fitView({ padding: 0.2, duration: 180 });
    });
    return () => {
      window.cancelAnimationFrame(frame);
    };
  }, [flow, layoutKey]);

  const selectedNode = snapshot?.nodes.find((node) => node.id === selected);
  const leader = snapshot?.nodes.find((node) => node.role === 'leader');

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">NODE RAFT RSM</p>
          <h1>Raft laboratory</h1>
        </div>
        <div className="cluster-setup">
          <label>
            Nodes
            <input
              type="number"
              min="1"
              max="9"
              value={nodeCount}
              onChange={(event) => {
                setNodeCount(event.currentTarget.valueAsNumber);
              }}
            />
          </label>
          <label>
            Seed
            <input
              type="number"
              value={seed}
              onChange={(event) => {
                setSeed(event.currentTarget.valueAsNumber);
              }}
            />
          </label>
          <button className="primary" onClick={() => void act({ type: 'reset', nodeCount, seed })}>
            New cluster
          </button>
        </div>
      </header>

      {error === undefined ? null : <div className="error-banner">{error}</div>}

      <section className="workspace">
        <aside className="panel controls-panel">
          <PanelTitle eyebrow="CONTROL" title="Simulation" />
          <label>
            Target node
            <select
              value={selected}
              onChange={(event) => {
                setSelected(event.currentTarget.value);
              }}
            >
              {snapshot?.nodes.map((node) => (
                <option key={node.id}>{node.id}</option>
              ))}
            </select>
          </label>
          <div className="button-grid">
            <button
              title="Immediately start a Raft leader election on the selected node"
              onClick={() => void act({ type: 'campaign', nodeId: selected })}
            >
              Start election
            </button>
            <button onClick={() => void act({ type: 'heartbeat', nodeId: selected })}>
              Heartbeat
            </button>
            <button
              disabled={!selectedNode?.online}
              onClick={() => void act({ type: 'disable', nodeId: selected })}
            >
              Disable
            </button>
            <button
              disabled={selectedNode?.online !== false}
              onClick={() => void act({ type: 'restart', nodeId: selected })}
            >
              Restart
            </button>
          </div>
          {leader === undefined ? (
            <p className="control-hint">Start an election and deliver the vote messages first.</p>
          ) : null}

          <div className="separator" />
          <p className="field-title">Propose KV command</p>
          <label>
            Key
            <input
              value={key}
              onChange={(event) => {
                setKey(event.currentTarget.value);
              }}
            />
          </label>
          <label>
            Value
            <input
              value={value}
              onChange={(event) => {
                setValue(event.currentTarget.value);
              }}
            />
          </label>
          <div className="button-grid">
            <button
              className="primary"
              disabled={leader === undefined}
              onClick={() =>
                leader === undefined
                  ? undefined
                  : void act({ type: 'propose', nodeId: leader.id, command: 'put', key, value })
              }
            >
              Put
            </button>
            <button
              disabled={leader === undefined}
              onClick={() =>
                leader === undefined
                  ? undefined
                  : void act({ type: 'propose', nodeId: leader.id, command: 'delete', key })
              }
            >
              Delete
            </button>
          </div>

          <div className="separator" />
          <p className="field-title">Partition group A</p>
          <div className="check-list">
            {snapshot?.nodes.map((node) => (
              <label className="check" key={node.id}>
                <input
                  type="checkbox"
                  checked={partition.includes(node.id)}
                  onChange={() => {
                    setPartition(toggle(partition, node.id));
                  }}
                />
                {node.id}
              </label>
            ))}
          </div>
          <div className="button-grid">
            <button onClick={() => void act({ type: 'partition', left: partition })}>
              Partition
            </button>
            <button onClick={() => void act({ type: 'heal' })}>Heal</button>
          </div>
        </aside>

        <section className="cluster-stage">
          <div className="stage-toolbar">
            <div className="legend">
              <i className="leader" /> Leader <i className="candidate" /> Candidate{' '}
              <i className="follower" /> Follower <i className="offline" /> Offline
            </div>
            <span className="interaction-guide">Click to select · hold to move</span>
            <div className="run-controls">
              <button onClick={() => void act({ type: 'step' })}>Step</button>
              <button onClick={() => void act({ type: 'drain' })}>Drain</button>
              <button
                className={running ? 'danger' : 'primary'}
                onClick={() => {
                  setRunning(!running);
                }}
              >
                {running ? 'Pause' : 'Auto run'}
              </button>
              <select
                aria-label="Run speed"
                value={speed}
                onChange={(event) => {
                  setSpeed(Number(event.currentTarget.value));
                }}
              >
                <option value="1000">1×</option>
                <option value="500">2×</option>
                <option value="150">Fast</option>
              </select>
            </div>
          </div>
          <ReactFlow
            nodes={flowNodes}
            edges={flowEdges}
            nodeTypes={nodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={(_event, node) => {
              setSelected(node.id);
            }}
            onInit={setFlow}
            fitView
            minZoom={0.45}
            maxZoom={1.5}
            nodesDraggable={false}
            nodesConnectable={false}
            elementsSelectable={false}
          >
            <Background color="#203149" gap={28} size={1} />
            <Controls showInteractive={false} />
          </ReactFlow>
        </section>

        <aside className="panel events-panel">
          <PanelTitle
            eyebrow="TRACE"
            title={`Messages · ${snapshot?.messages.length.toString() ?? '0'}`}
          />
          <div className="message-list">
            {snapshot?.messages.length === 0 ? (
              <Empty text="Queue is empty" />
            ) : (
              snapshot?.messages.map((message) => (
                <article className="message" key={message.id}>
                  <div>
                    <strong>
                      #{message.id} {shortType(message.type)}
                    </strong>
                    <span>T{message.term}</span>
                  </div>
                  <p>
                    {message.from} → {message.to}
                  </p>
                  <small>{message.detail}</small>
                  <div className="mini-actions">
                    <button onClick={() => void act({ type: 'deliver', messageId: message.id })}>
                      Deliver
                    </button>
                    <button onClick={() => void act({ type: 'duplicate', messageId: message.id })}>
                      Copy
                    </button>
                    <button onClick={() => void act({ type: 'drop', messageId: message.id })}>
                      Drop
                    </button>
                  </div>
                </article>
              ))
            )}
          </div>
          <button
            className="wide"
            disabled={(snapshot?.messages.length ?? 0) < 2}
            onClick={() => void act({ type: 'reorder' })}
          >
            Reverse queue
          </button>
          <div className="separator" />
          <div className="timeline-heading">
            <PanelTitle eyebrow="EVENTS" title="Timeline" />
            <span>{snapshot?.events.length.toString() ?? '0'} events · newest first</span>
          </div>
          <div className="timeline">
            {[...(snapshot?.events ?? [])].reverse().map((event) => (
              <div className={`timeline-event ${event.kind}`} key={event.id}>
                <i />
                <div className="timeline-content">
                  <div className="timeline-meta">
                    <strong>{eventLabel(event.kind)}</strong>
                    <time>{formatEventTime(event.at)}</time>
                  </div>
                  <p>{event.message}</p>
                </div>
              </div>
            ))}
          </div>
        </aside>
      </section>
    </main>
  );
}

function RaftNodeCard({ id, data }: NodeProps<RaftFlowNode>) {
  const { snapshot } = data;
  const { getNode, screenToFlowPosition } = useReactFlow<RaftFlowNode>();
  const longPressTimer = useRef<number | null>(null);
  const drag = useRef<{
    readonly pointerId: number;
    readonly offset: FlowPosition;
    readonly start: FlowPosition;
    active: boolean;
    cancelled: boolean;
  } | null>(null);
  const [moving, setMoving] = useState(false);

  useEffect(
    () => () => {
      if (longPressTimer.current !== null) window.clearTimeout(longPressTimer.current);
    },
    [],
  );

  const clearLongPress = useCallback(() => {
    if (longPressTimer.current !== null) window.clearTimeout(longPressTimer.current);
    longPressTimer.current = null;
  }, []);

  const handlePointerDown = (event: ReactPointerEvent<HTMLDivElement>): void => {
    if (event.button !== 0) return;
    const node = getNode(id);
    if (node === undefined) return;
    const pointer = screenToFlowPosition({ x: event.clientX, y: event.clientY });
    const element = event.currentTarget;
    drag.current = {
      pointerId: event.pointerId,
      offset: { x: pointer.x - node.position.x, y: pointer.y - node.position.y },
      start: { x: event.clientX, y: event.clientY },
      active: false,
      cancelled: false,
    };
    clearLongPress();
    longPressTimer.current = window.setTimeout(() => {
      const current = drag.current;
      if (current === null || current.cancelled) return;
      current.active = true;
      data.onSelect(id);
      setMoving(true);
      element.setPointerCapture(current.pointerId);
    }, 400);
  };

  const handlePointerMove = (event: ReactPointerEvent<HTMLDivElement>): void => {
    const current = drag.current;
    if (current === null) return;
    if (!current.active) {
      if (Math.hypot(event.clientX - current.start.x, event.clientY - current.start.y) > 6) {
        current.cancelled = true;
        clearLongPress();
      }
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    const pointer = screenToFlowPosition({ x: event.clientX, y: event.clientY });
    data.onMove(id, {
      x: pointer.x - current.offset.x,
      y: pointer.y - current.offset.y,
    });
  };

  const finishPointer = (event: ReactPointerEvent<HTMLDivElement>): void => {
    const current = drag.current;
    clearLongPress();
    drag.current = null;
    setMoving(false);
    if (current === null) return;
    if (event.currentTarget.hasPointerCapture(current.pointerId))
      event.currentTarget.releasePointerCapture(current.pointerId);
    if (current.active) {
      event.preventDefault();
      event.stopPropagation();
    }
  };

  return (
    <div
      className={`raft-node nodrag nopan ${snapshot.role}${data.selected ? ' selected' : ''}${moving ? ' moving' : ''}`}
      role="button"
      tabIndex={0}
      aria-pressed={data.selected}
      title="Click to select. Hold to move."
      onClick={(event) => {
        event.stopPropagation();
        data.onSelect(id);
      }}
      onPointerDown={handlePointerDown}
      onPointerMove={handlePointerMove}
      onPointerUp={(event) => {
        finishPointer(event);
      }}
      onPointerCancel={(event) => {
        finishPointer(event);
      }}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          data.onSelect(id);
        }
      }}
    >
      <Handle type="target" position={Position.Left} />
      <div className="node-head">
        <span className="role-dot" />
        <strong>{snapshot.id}</strong>
        <em>{snapshot.role}</em>
      </div>
      <div className="term">
        TERM <b>{snapshot.term}</b>
      </div>
      <dl>
        <div>
          <dt>Log</dt>
          <dd>{snapshot.lastLogIndex}</dd>
        </div>
        <div>
          <dt>Commit</dt>
          <dd>{snapshot.commitIndex}</dd>
        </div>
        <div>
          <dt>Applied</dt>
          <dd>{snapshot.appliedIndex}</dd>
        </div>
      </dl>
      <div className="state-values">
        {Object.keys(snapshot.values).length === 0
          ? 'state: ∅'
          : Object.entries(snapshot.values).map(([key, value]) => (
              <span key={key}>
                {key} = {value}
              </span>
            ))}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

function buildGraph(
  snapshot: ClusterSnapshot | undefined,
  selectedNodeId: string,
  onSelect: (nodeId: string) => void,
  onMove: (nodeId: string, position: FlowPosition) => void,
): {
  nodes: RaftFlowNode[];
  edges: Edge[];
} {
  if (snapshot === undefined) return { nodes: [], edges: [] };
  const radius = Math.max(190, snapshot.nodes.length * 34);
  const nodes = snapshot.nodes.map((node, index): RaftFlowNode => {
    const angle = (Math.PI * 2 * index) / snapshot.nodes.length - Math.PI / 2;
    return {
      id: node.id,
      type: 'raft',
      position: { x: Math.cos(angle) * radius, y: Math.sin(angle) * radius },
      data: { snapshot: node, selected: node.id === selectedNodeId, onSelect, onMove },
    };
  });
  const blocked = new Set(snapshot.blockedEdges.map((edge) => `${edge.from}:${edge.to}`));
  const edges: Edge[] = [];
  for (let left = 0; left < snapshot.nodes.length; left += 1) {
    for (let right = left + 1; right < snapshot.nodes.length; right += 1) {
      const from = snapshot.nodes[left];
      const to = snapshot.nodes[right];
      if (from === undefined || to === undefined) continue;
      const partitioned = blocked.has(`${from.id}:${to.id}`);
      const pending = snapshot.messages.filter(
        (message) =>
          (message.from === from.id && message.to === to.id) ||
          (message.from === to.id && message.to === from.id),
      ).length;
      edges.push({
        id: `${from.id}-${to.id}`,
        source: from.id,
        target: to.id,
        animated: pending > 0 && !partitioned,
        label: pending > 0 ? pending.toString() : undefined,
        className: partitioned ? 'partitioned-edge' : 'healthy-edge',
      });
    }
  }
  return { nodes, edges };
}

function PanelTitle({ eyebrow, title }: { readonly eyebrow: string; readonly title: string }) {
  return (
    <div className="panel-title">
      <span>{eyebrow}</span>
      <h2>{title}</h2>
    </div>
  );
}

function Empty({ text }: { readonly text: string }) {
  return <p className="empty">{text}</p>;
}
function toggle(values: readonly string[], value: string): readonly string[] {
  return values.includes(value) ? values.filter((item) => item !== value) : [...values, value];
}
function shortType(value: string): string {
  return value.replace('-request', ' req').replace('-response', ' res').replaceAll('-', ' ');
}
function eventLabel(kind: ClusterSnapshot['events'][number]['kind']): string {
  switch (kind) {
    case 'system':
      return 'System';
    case 'election':
      return 'Consensus';
    case 'command':
      return 'Command';
    case 'network':
      return 'Network';
    case 'node':
      return 'Node';
    case 'error':
      return 'Error';
  }
}
function formatEventTime(value: string): string {
  return new Date(value).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}
function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
