import type { EventKind, NormalizedEvent } from '../ingest/types.js';

export const DEFAULT_TICK_BUFFER_CAPACITY = 50_000;

export interface TickBufferCounters {
  readonly capacity: number;
  readonly size: number;
  readonly accepted: number;
  readonly droppedOldest: number;
  readonly droppedOldestByKind: Readonly<Record<'trade' | 'bbo', number>>;
  readonly refusedDepth: number;
  readonly refusedDroppable: number;
}

export interface TickBufferOptions<T extends NormalizedEvent> {
  capacity?: number;
  onRefuse?: (item: T) => void;
}

interface Node<T> {
  item: T;
  prev: Node<T> | null;
  next: Node<T> | null;
}

function isDroppable(kind: EventKind): kind is 'trade' | 'bbo' {
  return kind === 'trade' || kind === 'bbo';
}

export class TickBuffer<T extends NormalizedEvent> {
  readonly capacity: number;

  private head: Node<T> | null = null;
  private tail: Node<T> | null = null;
  private count = 0;
  private droppableCount = 0;
  private accepted = 0;
  private droppedOldestCount = 0;
  private readonly droppedOldestByKind: Record<'trade' | 'bbo', number> = { trade: 0, bbo: 0 };
  private refusedDepthCount = 0;
  private refusedDroppableCount = 0;
  private readonly onRefuse?: (item: T) => void;

  constructor(opts: TickBufferOptions<T> = {}) {
    this.capacity = opts.capacity ?? DEFAULT_TICK_BUFFER_CAPACITY;
    if (!Number.isSafeInteger(this.capacity) || this.capacity < 1) {
      throw new RangeError(`capacity must be a positive safe integer, got ${this.capacity}`);
    }
    this.onRefuse = opts.onRefuse;
  }

  push(item: T): boolean {
    if (this.count < this.capacity) {
      this.append(item);
      return true;
    }
    if (!isDroppable(item.kind)) {
      if (this.droppableCount === 0) {
        this.refusedDepthCount += 1;
        this.onRefuse?.(item);
        return false;
      }
      this.shedOldestDroppable();
      this.append(item);
      return true;
    }
    if (this.shedOldestDroppable()) {
      this.append(item);
      return true;
    }
    this.refusedDroppableCount += 1;
    this.onRefuse?.(item);
    return false;
  }

  drainUpTo(n: number): T[] {
    const out: T[] = [];
    const limit = Math.max(0, Math.min(n, this.count));
    for (let i = 0; i < limit; i++) {
      const node = this.head;
      if (!node) break;
      this.head = node.next;
      if (this.head) this.head.prev = null;
      else this.tail = null;
      this.count -= 1;
      if (isDroppable(node.item.kind)) this.droppableCount -= 1;
      out.push(node.item);
    }
    return out;
  }

  size(): number {
    return this.count;
  }

  counters(): TickBufferCounters {
    return {
      capacity: this.capacity,
      size: this.count,
      accepted: this.accepted,
      droppedOldest: this.droppedOldestCount,
      droppedOldestByKind: { ...this.droppedOldestByKind },
      refusedDepth: this.refusedDepthCount,
      refusedDroppable: this.refusedDroppableCount,
    };
  }

  private append(item: T): void {
    const node: Node<T> = { item, prev: this.tail, next: null };
    if (this.tail) this.tail.next = node;
    else this.head = node;
    this.tail = node;
    this.count += 1;
    this.accepted += 1;
    if (isDroppable(item.kind)) this.droppableCount += 1;
  }

  private shedOldestDroppable(): boolean {
    if (this.droppableCount === 0) return false;
    let cursor = this.head;
    while (cursor) {
      if (isDroppable(cursor.item.kind)) {
        if (cursor.prev) cursor.prev.next = cursor.next;
        else this.head = cursor.next;
        if (cursor.next) cursor.next.prev = cursor.prev;
        else this.tail = cursor.prev;
        this.count -= 1;
        this.droppableCount -= 1;
        this.droppedOldestCount += 1;
        this.droppedOldestByKind[cursor.item.kind] += 1;
        return true;
      }
      cursor = cursor.next;
    }
    return false;
  }
}
