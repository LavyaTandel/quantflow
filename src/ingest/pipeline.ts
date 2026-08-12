import type { EventKind, NormalizedEvent } from './types.js';

export interface PipelineOptions {
  capacity?: number;
}

export interface PipelineCounters {
  readonly accepted: number;
  readonly droppedByKind: Readonly<Record<EventKind, number>>;
  readonly currentDepth: number;
  readonly capacity: number;
}

export const DEFAULT_PIPELINE_CAPACITY = 10_000;

interface Node {
  event: NormalizedEvent;
  prev: Node | null;
  next: Node | null;
}

function zeroedDropCounters(): Record<EventKind, number> {
  return { trade: 0, bbo: 0, depth: 0 };
}

export class EventPipeline {
  readonly capacity: number;

  private head: Node | null = null;
  private tail: Node | null = null;
  private size = 0;
  private accepted = 0;
  private dropped = zeroedDropCounters();
  private closed = false;
  private readonly waiters: Array<() => void> = [];

  constructor(opts: PipelineOptions = {}) {
    this.capacity = opts.capacity ?? DEFAULT_PIPELINE_CAPACITY;
    if (!Number.isSafeInteger(this.capacity) || this.capacity < 1) {
      throw new RangeError(`capacity must be a positive safe integer, got ${this.capacity}`);
    }
  }

  push(event: NormalizedEvent): boolean {
    if (this.closed) return false;
    if (this.size < this.capacity) {
      this.append(event);
      return true;
    }
    if (event.kind === 'depth') {
      while (this.size >= this.capacity && this.shedOldestDroppable());
      if (this.size < this.capacity) {
        this.append(event);
        return true;
      }
      this.dropped.depth += 1;
      return false;
    }
    if (this.shedOldestDroppable()) {
      this.append(event);
      return true;
    }
    this.dropped[event.kind] += 1;
    return false;
  }

  pushMany(events: readonly NormalizedEvent[]): number {
    let admitted = 0;
    for (const event of events) {
      if (this.push(event)) admitted += 1;
    }
    return admitted;
  }

  take(): NormalizedEvent | undefined {
    const node = this.head;
    if (!node) return undefined;
    this.head = node.next;
    if (this.head) this.head.prev = null;
    else this.tail = null;
    this.size -= 1;
    return node.event;
  }

  async *drain(): AsyncGenerator<NormalizedEvent> {
    for (;;) {
      const event = this.take();
      if (event) {
        yield event;
        continue;
      }
      if (this.closed) return;
      await this.waitForSignal();
    }
  }

  close(): void {
    this.closed = true;
    this.wakeWaiters();
  }

  isClosed(): boolean {
    return this.closed;
  }

  counters(): PipelineCounters {
    return {
      accepted: this.accepted,
      droppedByKind: { ...this.dropped },
      currentDepth: this.size,
      capacity: this.capacity,
    };
  }

  private append(event: NormalizedEvent): void {
    const node: Node = { event, prev: this.tail, next: null };
    if (this.tail) this.tail.next = node;
    else this.head = node;
    this.tail = node;
    this.size += 1;
    this.accepted += 1;
    this.wakeWaiters();
  }

  private shedOldestDroppable(): boolean {
    let cursor = this.head;
    while (cursor) {
      const next: Node | null = cursor.next;
      if (cursor.event.kind === 'trade' || cursor.event.kind === 'bbo') {
        if (cursor.prev) cursor.prev.next = cursor.next;
        else this.head = cursor.next;
        if (cursor.next) cursor.next.prev = cursor.prev;
        else this.tail = cursor.prev;
        this.size -= 1;
        this.dropped[cursor.event.kind] += 1;
        return true;
      }
      cursor = next;
    }
    return false;
  }

  private waitForSignal(): Promise<void> {
    return new Promise((resolve) => {
      this.waiters.push(resolve);
    });
  }

  private wakeWaiters(): void {
    while (this.waiters.length > 0) {
      const wake = this.waiters.pop();
      wake?.();
    }
  }
}
