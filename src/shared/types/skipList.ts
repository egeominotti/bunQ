export interface SkipNode<T> {
  value: T;
  forward: Array<SkipNode<T> | null>;
}
