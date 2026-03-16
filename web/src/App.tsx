import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
  type PointerEvent as ReactPointerEvent,
  type WheelEvent as ReactWheelEvent,
} from "react";
import "./App.css";

type TxEdge = {
  hash: string;
  from: string;
  to: string;
  value_eth: string;
  block: number;
};

type WsNode = {
  address: string;
};

type WsResponse = {
  type: string;
  address?: string;
  nodes?: WsNode[];
  edges?: TxEdge[];
  offset?: number;
  count?: number;
  has_more?: boolean;
  source?: string;
  error?: string;
};

type VizNode = {
  address: string;
  x: number;
  y: number;
  degree: number;
  expanded: boolean;
  pending: boolean;
  hasMore: boolean;
  nextOffset: number;
};

const ADDRESS_PATTERN = /^0x[a-fA-F0-9]{40}$/;
const QUERY_LIMIT = 80;

function normalizeAddress(value: string): string | null {
  const trimmed = value.trim().toLowerCase();
  if (!trimmed) {
    return null;
  }
  const withPrefix = trimmed.startsWith("0x") ? trimmed : `0x${trimmed}`;
  if (!ADDRESS_PATTERN.test(withPrefix)) {
    return null;
  }
  return withPrefix;
}

function shortAddress(address: string): string {
  if (address.length < 12) {
    return address;
  }
  return `${address.slice(0, 6)}…${address.slice(-4)}`;
}

function edgeKey(edge: TxEdge): string {
  return `${edge.hash}|${edge.from}|${edge.to}|${edge.block}|${edge.value_eth}`;
}

function buildWebSocketURL(endpoint: string): string {
  if (endpoint.startsWith("ws://") || endpoint.startsWith("wss://")) {
    return endpoint;
  }
  if (endpoint.startsWith("http://")) {
    return `ws://${endpoint.slice("http://".length)}`;
  }
  if (endpoint.startsWith("https://")) {
    return `wss://${endpoint.slice("https://".length)}`;
  }

  const prefix = window.location.protocol === "https:" ? "wss://" : "ws://";
  if (endpoint.startsWith("/")) {
    return `${prefix}${window.location.host}${endpoint}`;
  }
  return `${prefix}${window.location.host}/${endpoint}`;
}

function placeNearAnchor(
  anchor: VizNode | undefined,
  width: number,
  height: number,
): { x: number; y: number } {
  const centerX = width / 2;
  const centerY = height / 2;
  if (!anchor) {
    return {
      x: centerX + (Math.random() - 0.5) * width * 0.5,
      y: centerY + (Math.random() - 0.5) * height * 0.5,
    };
  }

  const angle = Math.random() * Math.PI * 2;
  const radius = 150 + Math.random() * 170;
  return {
    x: anchor.x + Math.cos(angle) * radius,
    y: anchor.y + Math.sin(angle) * radius,
  };
}

function clamp(value: number, min: number, max: number): number {
  if (value < min) {
    return min;
  }
  if (value > max) {
    return max;
  }
  return value;
}

function formatEthAmount(value: number): string {
  if (!Number.isFinite(value) || value === 0) {
    return "0";
  }

  const abs = Math.abs(value);
  const maxFractionDigits = abs >= 100 ? 2 : abs >= 1 ? 4 : 6;
  return value.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: maxFractionDigits,
  });
}

function relaxLayout(
  nodesByAddress: Record<string, VizNode>,
  edges: TxEdge[],
  width: number,
  height: number,
  focusAddress: string,
): void {
  const nodes = Object.values(nodesByAddress);
  if (nodes.length <= 1) {
    return;
  }

  const nodeMap = new Map<string, VizNode>();
  for (const node of nodes) {
    nodeMap.set(node.address, node);
  }

  const edgesToUse = edges.slice(Math.max(0, edges.length - 900));
  const centerX = width / 2;
  const centerY = height / 2;

  for (let i = 0; i < 24; i += 1) {
    for (const edge of edgesToUse) {
      const from = nodeMap.get(edge.from);
      const to = nodeMap.get(edge.to);
      if (!from || !to) {
        continue;
      }
      const dx = to.x - from.x;
      const dy = to.y - from.y;
      const distance = Math.max(0.001, Math.hypot(dx, dy));
      const target = 85;
      const spring = (distance - target) * 0.015;
      const fx = (dx / distance) * spring;
      const fy = (dy / distance) * spring;

      from.x += fx;
      from.y += fy;
      to.x -= fx;
      to.y -= fy;
    }

    for (let idx = 0; idx < nodes.length; idx += 1) {
      const node = nodes[idx];

      const centerPull = node.address === focusAddress ? 0.035 : 0.01;
      node.x += (centerX - node.x) * centerPull;
      node.y += (centerY - node.y) * centerPull;

      for (let k = 1; k <= 7 && nodes.length > 1; k += 1) {
        const other = nodes[(idx + k * 17 + i * 5) % nodes.length];
        if (other.address === node.address) {
          continue;
        }
        const dx = node.x - other.x;
        const dy = node.y - other.y;
        const sq = Math.max(60, dx * dx + dy * dy);
        const repel = 1800 / sq;
        node.x += (dx / Math.sqrt(sq)) * repel;
        node.y += (dy / Math.sqrt(sq)) * repel;
      }

      node.x = clamp(node.x, 28, width - 28);
      node.y = clamp(node.y, 28, height - 28);
    }
  }
}

function App() {
  const apiBase = useMemo(() => {
    const envBase = (
      import.meta.env.VITE_API_BASE as string | undefined
    )?.trim();
    return (envBase || "/api").replace(/\/$/, "");
  }, []);

  const [addressInput, setAddressInput] = useState("");
  const [statusText, setStatusText] = useState("Connecting websocket...");
  const [isConnected, setIsConnected] = useState(false);
  const [selectedAddress, setSelectedAddress] = useState("");
  const [lastSource, setLastSource] = useState("");
  const [nodesByAddress, setNodesByAddress] = useState<Record<string, VizNode>>(
    {},
  );
  const [edges, setEdges] = useState<TxEdge[]>([]);
  const [canvasSize, setCanvasSize] = useState({ width: 920, height: 660 });
  const [viewTransform, setViewTransform] = useState({
    panX: 0,
    panY: 0,
    zoom: 1,
  });
  const [isPointerDragging, setIsPointerDragging] = useState(false);

  const graphRef = useRef<HTMLDivElement | null>(null);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const edgeKeysRef = useRef<Set<string>>(new Set());
  const edgesRef = useRef<TxEdge[]>([]);
  const viewRef = useRef(viewTransform);
  const reconnectTimerRef = useRef<number | null>(null);
  const reconnectAllowedRef = useRef(true);
  const pendingNodeDragRef = useRef<{
    address: string;
    x: number;
    y: number;
  } | null>(null);
  const nodeDragRafRef = useRef<number | null>(null);
  const dragRef = useRef({
    pointerId: -1,
    mode: "none" as "none" | "pan" | "node",
    nodeAddress: "",
    lastX: 0,
    lastY: 0,
    moved: false,
    grabOffsetX: 0,
    grabOffsetY: 0,
  });

  useEffect(() => {
    viewRef.current = viewTransform;
  }, [viewTransform]);

  useEffect(
    () => () => {
      if (nodeDragRafRef.current !== null) {
        window.cancelAnimationFrame(nodeDragRafRef.current);
      }
    },
    [],
  );

  const toSvgPoint = useCallback(
    (clientX: number, clientY: number) => {
      const svg = svgRef.current;
      if (!svg) {
        return { x: 0, y: 0 };
      }
      const rect = svg.getBoundingClientRect();
      if (rect.width === 0 || rect.height === 0) {
        return { x: 0, y: 0 };
      }

      return {
        x: ((clientX - rect.left) * canvasSize.width) / rect.width,
        y: ((clientY - rect.top) * canvasSize.height) / rect.height,
      };
    },
    [canvasSize.height, canvasSize.width],
  );

  const zoomAt = useCallback(
    (factor: number, pivot?: { x: number; y: number }) => {
      setViewTransform((prev) => {
        const nextZoom = clamp(prev.zoom * factor, 0.3, 5);
        if (nextZoom === prev.zoom) {
          return prev;
        }

        const focus =
          pivot ||
          ({ x: canvasSize.width / 2, y: canvasSize.height / 2 } as const);
        const worldX = (focus.x - prev.panX) / prev.zoom;
        const worldY = (focus.y - prev.panY) / prev.zoom;

        return {
          zoom: nextZoom,
          panX: focus.x - worldX * nextZoom,
          panY: focus.y - worldY * nextZoom,
        };
      });
    },
    [canvasSize.height, canvasSize.width],
  );

  const handleWheelZoom = useCallback(
    (event: ReactWheelEvent<SVGSVGElement>) => {
      event.preventDefault();
      const pivot = toSvgPoint(event.clientX, event.clientY);
      const factor = event.deltaY < 0 ? 1.12 : 0.9;
      zoomAt(factor, pivot);
    },
    [toSvgPoint, zoomAt],
  );

  const sendWs = useCallback((payload: object): boolean => {
    const socket = wsRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      setStatusText("WebSocket is not connected");
      return false;
    }
    socket.send(JSON.stringify(payload));
    return true;
  }, []);

  const resetGraph = useCallback(
    (seed: string) => {
      edgeKeysRef.current = new Set();
      edgesRef.current = [];
      setEdges([]);
      setViewTransform({ panX: 0, panY: 0, zoom: 1 });

      const centerNode: VizNode = {
        address: seed,
        x: canvasSize.width / 2,
        y: canvasSize.height / 2,
        degree: 0,
        expanded: false,
        pending: true,
        hasMore: false,
        nextOffset: 0,
      };
      setNodesByAddress({ [seed]: centerNode });
      setSelectedAddress(seed);
    },
    [canvasSize.height, canvasSize.width],
  );

  const ingestSubgraph = useCallback(
    (message: WsResponse) => {
      const requestAddress = normalizeAddress(message.address || "") || "";
      const incomingEdges = message.edges || [];
      const incomingNodes = message.nodes || [];

      const addedEdges: TxEdge[] = [];
      for (const rawEdge of incomingEdges) {
        const from = normalizeAddress(rawEdge.from) || "";
        const to = normalizeAddress(rawEdge.to) || "";
        if (!from || !to) {
          continue;
        }
        const normalized: TxEdge = {
          hash: rawEdge.hash,
          from,
          to,
          value_eth: rawEdge.value_eth,
          block: rawEdge.block,
        };
        const key = edgeKey(normalized);
        if (edgeKeysRef.current.has(key)) {
          continue;
        }
        edgeKeysRef.current.add(key);
        addedEdges.push(normalized);
      }

      if (addedEdges.length > 0) {
        edgesRef.current = [...edgesRef.current, ...addedEdges];
        setEdges(edgesRef.current);
      }

      setNodesByAddress((previous) => {
        const next: Record<string, VizNode> = { ...previous };
        const previousCount = Object.keys(previous).length;
        const anchor = requestAddress ? next[requestAddress] : undefined;

        for (const node of incomingNodes) {
          const addr = normalizeAddress(node.address);
          if (!addr || next[addr]) {
            continue;
          }
          const point = placeNearAnchor(
            anchor,
            canvasSize.width,
            canvasSize.height,
          );
          next[addr] = {
            address: addr,
            x: point.x,
            y: point.y,
            degree: 0,
            expanded: false,
            pending: false,
            hasMore: false,
            nextOffset: 0,
          };
        }

        for (const edge of addedEdges) {
          if (!next[edge.from]) {
            const point = placeNearAnchor(
              anchor,
              canvasSize.width,
              canvasSize.height,
            );
            next[edge.from] = {
              address: edge.from,
              x: point.x,
              y: point.y,
              degree: 0,
              expanded: false,
              pending: false,
              hasMore: false,
              nextOffset: 0,
            };
          }
          if (!next[edge.to]) {
            const point = placeNearAnchor(
              anchor,
              canvasSize.width,
              canvasSize.height,
            );
            next[edge.to] = {
              address: edge.to,
              x: point.x,
              y: point.y,
              degree: 0,
              expanded: false,
              pending: false,
              hasMore: false,
              nextOffset: 0,
            };
          }
          next[edge.from].degree += 1;
          next[edge.to].degree += 1;
        }

        if (requestAddress && next[requestAddress]) {
          next[requestAddress] = {
            ...next[requestAddress],
            expanded: true,
            pending: false,
            hasMore: Boolean(message.has_more),
            nextOffset:
              (message.offset || 0) + (message.count || incomingEdges.length),
          };
        }

        if (previousCount <= 1) {
          relaxLayout(
            next,
            edgesRef.current,
            canvasSize.width,
            canvasSize.height,
            requestAddress,
          );
        }
        return next;
      });

      if (requestAddress) {
        setSelectedAddress(requestAddress);
      }

      setLastSource(message.source || "unknown");
      setStatusText(
        `Loaded ${message.count || incomingEdges.length} edge(s) from ${message.source || "api"}`,
      );
    },
    [canvasSize.height, canvasSize.width],
  );

  const connectWebSocket = useCallback(() => {
    const endpoint = `${apiBase}/ws/graph`;
    const wsURL = buildWebSocketURL(endpoint);

    if (wsRef.current) {
      wsRef.current.close();
    }

    const socket = new WebSocket(wsURL);
    wsRef.current = socket;
    setStatusText(`Connecting to ${wsURL}...`);

    socket.onopen = () => {
      setIsConnected(true);
      setStatusText("WebSocket connected");
    };
    socket.onclose = () => {
      setIsConnected(false);
      setStatusText("WebSocket disconnected");
      if (!reconnectAllowedRef.current) {
        return;
      }
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
      }
      reconnectTimerRef.current = window.setTimeout(() => {
        connectWebSocket();
      }, 1500);
    };
    socket.onerror = () => {
      setStatusText("WebSocket error");
    };
    socket.onmessage = (event) => {
      let parsed: WsResponse;
      try {
        parsed = JSON.parse(String(event.data)) as WsResponse;
      } catch {
        setStatusText("Invalid WebSocket payload");
        return;
      }

      if (parsed.type === "ready") {
        setStatusText("WebSocket ready");
        return;
      }
      if (parsed.type === "error") {
        setStatusText(parsed.error || "API error");
        return;
      }
      if (parsed.type === "subgraph") {
        ingestSubgraph(parsed);
      }
    };
  }, [apiBase, ingestSubgraph]);

  useEffect(() => {
    connectWebSocket();
    return () => {
      reconnectAllowedRef.current = false;
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [connectWebSocket]);

  const requestNode = useCallback(
    (action: "seed" | "expand", address: string, offset: number) => {
      const normalized = normalizeAddress(address);
      if (!normalized) {
        setStatusText("Invalid wallet address");
        return;
      }

      setNodesByAddress((previous) => {
        const next = { ...previous };
        if (next[normalized]) {
          next[normalized] = { ...next[normalized], pending: true };
        }
        return next;
      });

      if (
        !sendWs({ action, address: normalized, limit: QUERY_LIMIT, offset })
      ) {
        setNodesByAddress((previous) => {
          const next = { ...previous };
          if (next[normalized]) {
            next[normalized] = { ...next[normalized], pending: false };
          }
          return next;
        });
      }
    },
    [sendWs],
  );

  const startAddressQuery = useCallback(() => {
    const normalized = normalizeAddress(addressInput);
    if (!normalized) {
      setStatusText("Enter a valid wallet address");
      return;
    }
    if (!isConnected) {
      setStatusText("WebSocket is reconnecting, please wait");
      return;
    }

    if (Object.keys(nodesByAddress).length === 0) {
      resetGraph(normalized);
    } else {
      setNodesByAddress((previous) => {
        if (previous[normalized]) {
          return previous;
        }

        const anchor = selectedAddress ? previous[selectedAddress] : undefined;
        const point = placeNearAnchor(
          anchor,
          canvasSize.width,
          canvasSize.height,
        );
        return {
          ...previous,
          [normalized]: {
            address: normalized,
            x: point.x,
            y: point.y,
            degree: 0,
            expanded: false,
            pending: false,
            hasMore: false,
            nextOffset: 0,
          },
        };
      });
      setSelectedAddress(normalized);
    }

    requestNode("seed", normalized, 0);
  }, [
    addressInput,
    canvasSize.height,
    canvasSize.width,
    isConnected,
    nodesByAddress,
    requestNode,
    resetGraph,
    selectedAddress,
  ]);

  const onAddressKeyDown = useCallback(
    (event: ReactKeyboardEvent<HTMLInputElement>) => {
      if (event.key !== "Enter") {
        return;
      }
      event.preventDefault();
      startAddressQuery();
    },
    [startAddressQuery],
  );

  const handleSvgPointerDown = useCallback(
    (event: ReactPointerEvent<SVGSVGElement>) => {
      if (event.button !== 0) {
        return;
      }

      event.preventDefault();
      const point = toSvgPoint(event.clientX, event.clientY);
      dragRef.current = {
        pointerId: event.pointerId,
        mode: "pan",
        nodeAddress: "",
        lastX: point.x,
        lastY: point.y,
        moved: false,
        grabOffsetX: 0,
        grabOffsetY: 0,
      };
      setIsPointerDragging(true);
      event.currentTarget.setPointerCapture(event.pointerId);
    },
    [toSvgPoint],
  );

  const handleNodePointerDown = useCallback(
    (event: ReactPointerEvent<SVGGElement>, address: string) => {
      if (event.button !== 0) {
        return;
      }

      event.preventDefault();
      event.stopPropagation();
      const point = toSvgPoint(event.clientX, event.clientY);
      const view = viewRef.current;
      const worldX = (point.x - view.panX) / view.zoom;
      const worldY = (point.y - view.panY) / view.zoom;
      const node = nodesByAddress[address];
      const grabOffsetX = node ? node.x - worldX : 0;
      const grabOffsetY = node ? node.y - worldY : 0;

      dragRef.current = {
        pointerId: event.pointerId,
        mode: "node",
        nodeAddress: address,
        lastX: point.x,
        lastY: point.y,
        moved: false,
        grabOffsetX,
        grabOffsetY,
      };
      setIsPointerDragging(true);
      setSelectedAddress(address);
      svgRef.current?.setPointerCapture(event.pointerId);
    },
    [nodesByAddress, toSvgPoint],
  );

  const handleSvgPointerMove = useCallback(
    (event: ReactPointerEvent<SVGSVGElement>) => {
      const drag = dragRef.current;
      if (drag.pointerId !== event.pointerId || drag.mode === "none") {
        return;
      }

      event.preventDefault();
      const point = toSvgPoint(event.clientX, event.clientY);
      const dx = point.x - drag.lastX;
      const dy = point.y - drag.lastY;
      if (Math.hypot(dx, dy) > 1.25) {
        drag.moved = true;
      }

      if (drag.mode === "pan") {
        setViewTransform((prev) => ({
          ...prev,
          panX: prev.panX + dx,
          panY: prev.panY + dy,
        }));
      } else if (drag.mode === "node" && drag.nodeAddress) {
        const view = viewRef.current;
        const worldX = (point.x - view.panX) / view.zoom + drag.grabOffsetX;
        const worldY = (point.y - view.panY) / view.zoom + drag.grabOffsetY;
        pendingNodeDragRef.current = {
          address: drag.nodeAddress,
          x: clamp(worldX, 16, canvasSize.width - 16),
          y: clamp(worldY, 16, canvasSize.height - 16),
        };

        if (nodeDragRafRef.current === null) {
          nodeDragRafRef.current = window.requestAnimationFrame(() => {
            nodeDragRafRef.current = null;
            const pending = pendingNodeDragRef.current;
            if (!pending) {
              return;
            }
            setNodesByAddress((previous) => {
              const node = previous[pending.address];
              if (!node) {
                return previous;
              }
              if (node.x === pending.x && node.y === pending.y) {
                return previous;
              }
              return {
                ...previous,
                [pending.address]: {
                  ...node,
                  x: pending.x,
                  y: pending.y,
                },
              };
            });
          });
        }
      }

      drag.lastX = point.x;
      drag.lastY = point.y;
    },
    [canvasSize.height, canvasSize.width, toSvgPoint],
  );

  const finishPointerInteraction = useCallback(
    (event: ReactPointerEvent<SVGSVGElement>) => {
      const drag = dragRef.current;
      if (drag.pointerId !== event.pointerId || drag.mode === "none") {
        return;
      }

      if (nodeDragRafRef.current !== null) {
        window.cancelAnimationFrame(nodeDragRafRef.current);
        nodeDragRafRef.current = null;
      }

      const pending = pendingNodeDragRef.current;
      if (pending) {
        setNodesByAddress((previous) => {
          const node = previous[pending.address];
          if (!node) {
            return previous;
          }
          if (node.x === pending.x && node.y === pending.y) {
            return previous;
          }
          return {
            ...previous,
            [pending.address]: {
              ...node,
              x: pending.x,
              y: pending.y,
            },
          };
        });
        pendingNodeDragRef.current = null;
      }

      const wasClick = drag.mode === "node" && !drag.moved;
      const clickedAddress = drag.nodeAddress;
      dragRef.current = {
        pointerId: -1,
        mode: "none",
        nodeAddress: "",
        lastX: 0,
        lastY: 0,
        moved: false,
        grabOffsetX: 0,
        grabOffsetY: 0,
      };
      setIsPointerDragging(false);

      if (event.currentTarget.hasPointerCapture(event.pointerId)) {
        event.currentTarget.releasePointerCapture(event.pointerId);
      }

      if (wasClick && clickedAddress) {
        const node = nodesByAddress[clickedAddress];
        if (!node || node.pending) {
          return;
        }

        if (!node.expanded) {
          requestNode("expand", clickedAddress, 0);
        } else if (node.hasMore) {
          requestNode("expand", clickedAddress, node.nextOffset);
        }
      }
    },
    [nodesByAddress, requestNode],
  );

  const handleSvgPointerLeave = useCallback(
    (event: ReactPointerEvent<SVGSVGElement>) => {
      const drag = dragRef.current;
      if (drag.mode === "none" || drag.pointerId !== event.pointerId) {
        return;
      }
      if (event.currentTarget.hasPointerCapture(event.pointerId)) {
        return;
      }
      dragRef.current = {
        pointerId: -1,
        mode: "none",
        nodeAddress: "",
        lastX: 0,
        lastY: 0,
        moved: false,
        grabOffsetX: 0,
        grabOffsetY: 0,
      };
      setIsPointerDragging(false);
    },
    [],
  );

  const selectedNode = selectedAddress
    ? nodesByAddress[selectedAddress]
    : undefined;

  const selectedStats = useMemo(() => {
    if (!selectedAddress) {
      return null;
    }

    let incomingTransfers = 0;
    let outgoingTransfers = 0;
    let incomingEth = 0;
    let outgoingEth = 0;
    let latestBlock = 0;

    for (const edge of edges) {
      if (edge.to === selectedAddress) {
        incomingTransfers += 1;
        const value = Number(edge.value_eth);
        if (Number.isFinite(value)) {
          incomingEth += value;
        }
        latestBlock = Math.max(latestBlock, edge.block || 0);
      }
      if (edge.from === selectedAddress) {
        outgoingTransfers += 1;
        const value = Number(edge.value_eth);
        if (Number.isFinite(value)) {
          outgoingEth += value;
        }
        latestBlock = Math.max(latestBlock, edge.block || 0);
      }
    }

    return {
      incomingTransfers,
      outgoingTransfers,
      incomingEth,
      outgoingEth,
      totalTransfers: incomingTransfers + outgoingTransfers,
      latestBlock: latestBlock || null,
    };
  }, [edges, selectedAddress]);

  const nodeCount = useMemo(
    () => Object.keys(nodesByAddress).length,
    [nodesByAddress],
  );

  return (
    <div className="page-shell minimal">
      <header className="simple-header">
        <h1>Wallet Graph Explorer</h1>
        <input
          className="address-input"
          value={addressInput}
          onChange={(event) => setAddressInput(event.target.value)}
          onKeyDown={onAddressKeyDown}
          placeholder="Paste wallet address and press Enter (0x...)"
        />
        <div className="status-row">
          <span className={`pill ${isConnected ? "online" : "offline"}`}>
            {isConnected ? "Connected" : "Reconnecting"}
          </span>
          <span className="pill neutral">{statusText}</span>
          <span className="pill neutral">nodes {nodeCount}</span>
          <span className="pill neutral">edges {edges.length}</span>
          <span className="pill neutral">
            zoom {(viewTransform.zoom * 100).toFixed(0)}%
          </span>
          {lastSource ? (
            <span className="pill neutral">source {lastSource}</span>
          ) : null}
          {selectedNode ? (
            <span className="pill neutral">
              selected {shortAddress(selectedAddress)} · degree{" "}
              {selectedNode.degree}
            </span>
          ) : null}
        </div>
        <p className="hint">
          Click a node to expand. Click again for more edges if available. Drag
          canvas to pan, wheel to zoom, drag nodes to reposition.
        </p>
      </header>

      <section className="graph-panel" ref={graphRef}>
        <svg
          ref={svgRef}
          className="graph-svg"
          data-dragging={isPointerDragging ? "true" : "false"}
          viewBox={`0 0 ${canvasSize.width} ${canvasSize.height}`}
          preserveAspectRatio="xMidYMid meet"
          onWheel={handleWheelZoom}
          onPointerDown={handleSvgPointerDown}
          onPointerMove={handleSvgPointerMove}
          onPointerUp={finishPointerInteraction}
          onPointerCancel={finishPointerInteraction}
          onPointerLeave={handleSvgPointerLeave}
        >
          <defs>
            <marker
              id="arrow"
              markerWidth="8"
              markerHeight="8"
              refX="6"
              refY="3"
              orient="auto"
            >
              <path d="M0,0 L0,6 L6,3 z" className="arrow-head" />
            </marker>
          </defs>

          <g
            transform={`translate(${viewTransform.panX} ${viewTransform.panY}) scale(${viewTransform.zoom})`}
          >
            {edges.map((edge) => {
              const from = nodesByAddress[edge.from];
              const to = nodesByAddress[edge.to];
              if (!from || !to) {
                return null;
              }
              const highlighted =
                selectedAddress !== "" &&
                (edge.from === selectedAddress || edge.to === selectedAddress);
              return (
                <line
                  key={edgeKey(edge)}
                  x1={from.x}
                  y1={from.y}
                  x2={to.x}
                  y2={to.y}
                  className={highlighted ? "edge edge-highlight" : "edge"}
                  markerEnd="url(#arrow)"
                />
              );
            })}

            {Object.values(nodesByAddress).map((node) => {
              const isSelected = node.address === selectedAddress;
              const className = node.pending
                ? "node pending"
                : isSelected
                  ? "node selected"
                  : "node";

              return (
                <g
                  key={node.address}
                  transform={`translate(${node.x} ${node.y})`}
                  className={className}
                  onPointerDown={(event) =>
                    handleNodePointerDown(event, node.address)
                  }
                >
                  <circle r={isSelected ? 12 : 9} />
                  <text y={isSelected ? -17 : -15}>
                    {shortAddress(node.address)}
                  </text>
                </g>
              );
            })}
          </g>
        </svg>

        {selectedNode && selectedStats ? (
          <aside className="node-hover-card" aria-live="polite">
            <p className="node-hover-title">Selected wallet</p>
            <p className="node-hover-address">{selectedNode.address}</p>
            <div className="node-hover-grid">
              <span>Degree</span>
              <strong>{selectedNode.degree}</strong>
              <span>Transfers</span>
              <strong>{selectedStats.totalTransfers}</strong>
              <span>Incoming</span>
              <strong>{selectedStats.incomingTransfers}</strong>
              <span>Outgoing</span>
              <strong>{selectedStats.outgoingTransfers}</strong>
              <span>In ETH</span>
              <strong>{formatEthAmount(selectedStats.incomingEth)}</strong>
              <span>Out ETH</span>
              <strong>{formatEthAmount(selectedStats.outgoingEth)}</strong>
              <span>Expanded</span>
              <strong>{selectedNode.expanded ? "yes" : "no"}</strong>
              <span>Pending</span>
              <strong>{selectedNode.pending ? "yes" : "no"}</strong>
              <span>More edges</span>
              <strong>{selectedNode.hasMore ? "yes" : "no"}</strong>
              <span>Next offset</span>
              <strong>
                {selectedNode.hasMore ? selectedNode.nextOffset : "-"}
              </strong>
              <span>Latest block</span>
              <strong>{selectedStats.latestBlock ?? "-"}</strong>
            </div>
            <p className="node-hover-footnote">
              Click this node again to load more edges when available.
            </p>
          </aside>
        ) : null}
      </section>
    </div>
  );
}

export default App;
