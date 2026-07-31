<?php

declare(strict_types=1);

namespace Bunqueue\Flow;

use Bunqueue\Options;
use Bunqueue\Wire\Protocol;

/**
 * Pure compiler from public flow values to one fully-resolved PUSHF graph.
 *
 * @phpstan-type PlannedJob array{id: string, queue: string, input: array<string, mixed>}
 * @phpstan-type PlannedNode array{id: string, children: list<PlannedNode>}
 */
final class Planner
{
    private const MAX_DEPTH = 100;
    private const MAX_JOBS = 10_000;
    private const TOPOLOGY = ['parentId', 'dependsOn', 'childrenIds'];
    private const UNSUPPORTED = [
        'repeat',
        'uniqueKey',
        'dedup',
        'deduplication',
        'debounce',
        'debounceId',
        'debounceTtl',
    ];

    /** @var \Closure(): string */
    private readonly \Closure $idGenerator;

    /** @param (callable(): string)|null $idGenerator */
    public function __construct(?callable $idGenerator = null)
    {
        $this->idGenerator = $idGenerator === null
            ? self::randomId(...)
            : \Closure::fromCallable($idGenerator);
    }

    /**
     * @param array<string, mixed> $flow
     * @return array{jobs: list<PlannedJob>, root: PlannedNode}
     */
    public function planTree(array $flow): array
    {
        $jobs = [];
        $ids = [];
        $count = 0;
        $root = $this->visit($flow, null, 0, $jobs, $ids, $count);
        return ['jobs' => $jobs, 'root' => $root];
    }

    /**
     * @param list<array<string, mixed>> $steps
     * @return array{jobs: list<PlannedJob>, ids: list<string>}
     */
    public function planChain(array $steps): array
    {
        if (!array_is_list($steps)) {
            throw new \InvalidArgumentException('flow steps must be a list');
        }
        if (\count($steps) > self::MAX_JOBS) {
            throw new \InvalidArgumentException('flow exceeds the 10000 job limit');
        }

        $seen = [];
        $ids = [];
        foreach ($steps as $step) {
            $this->validateNode($step);
            if (($step['children'] ?? []) !== []) {
                throw new \InvalidArgumentException('nested children are not supported by addChain');
            }
            [$id] = $this->allocate($step, $seen);
            $ids[] = $id;
        }

        $jobs = [];
        foreach ($steps as $index => $step) {
            $dependency = $index > 0 ? $ids[$index - 1] : null;
            $data = $this->dataFor($step, [
                '__flowParentId' => $dependency,
            ]);
            $jobs[] = [
                'id' => $ids[$index],
                'queue' => $step['queueName'],
                'input' => $this->input(
                    $step['opts'] ?? [],
                    $data,
                    $ids[$index],
                    null,
                    $dependency === null ? [] : [$dependency],
                    [],
                ),
            ];
        }
        return ['jobs' => $jobs, 'ids' => $ids];
    }

    /**
     * @param array<string, mixed> $node
     * @param array{id: string, queue: string}|null $parent
     * @param list<PlannedJob> $jobs
     * @param array<string, true> $ids
     * @return PlannedNode
     */
    private function visit(
        array $node,
        ?array $parent,
        int $depth,
        array &$jobs,
        array &$ids,
        int &$count,
    ): array {
        $this->validateNode($node);
        if ($depth > self::MAX_DEPTH) {
            throw new \InvalidArgumentException('flow exceeds the 100 level depth limit');
        }
        if (++$count > self::MAX_JOBS) {
            throw new \InvalidArgumentException('flow exceeds the 10000 job limit');
        }
        [$id] = $this->allocate($node, $ids);

        $children = [];
        foreach ($node['children'] ?? [] as $child) {
            if (!\is_array($child)) {
                throw new \InvalidArgumentException('flow node must be an array');
            }
            $children[] = $this->visit(
                $child,
                ['id' => $id, 'queue' => $node['queueName']],
                $depth + 1,
                $jobs,
                $ids,
                $count,
            );
        }
        $childIds = array_column($children, 'id');
        $internal = [];
        if ($parent !== null) {
            $internal['__parentId'] = $parent['id'];
            $internal['__parentQueue'] = $parent['queue'];
        }
        if ($childIds !== []) {
            $internal['__childrenIds'] = $childIds;
        }
        $jobs[] = [
            'id' => $id,
            'queue' => $node['queueName'],
            'input' => $this->input(
                $node['opts'] ?? [],
                $this->dataFor($node, $internal),
                $id,
                $parent['id'] ?? null,
                $childIds,
                $childIds,
            ),
        ];
        return ['id' => $id, 'children' => $children];
    }

    /** @param array<string, mixed> $node */
    private function validateNode(array $node): void
    {
        $name = $node['name'] ?? null;
        if (!\is_string($name) || $name === '' || \strlen($name) > 256) {
            throw new \InvalidArgumentException(
                'flow job name must be a non-empty string of at most 256 characters'
            );
        }
        $queue = $node['queueName'] ?? null;
        if (
            !\is_string($queue) ||
            $queue === '' ||
            \strlen($queue) > 256 ||
            preg_match('/^[a-zA-Z0-9_.:-]+$/D', $queue) !== 1
        ) {
            throw new \InvalidArgumentException('flow queueName is invalid');
        }
        if (
            \array_key_exists('children', $node) &&
            (!\is_array($node['children']) || !array_is_list($node['children']))
        ) {
            throw new \InvalidArgumentException('flow children must be a list');
        }
        if (isset($node['opts']) && !\is_array($node['opts'])) {
            throw new \InvalidArgumentException('flow opts must be an array');
        }
    }

    /**
     * @param array<string, mixed> $node
     * @param array<string, true> $seen
     * @return array{string, bool}
     */
    private function allocate(array $node, array &$seen): array
    {
        $opts = $node['opts'] ?? [];
        $explicit = \array_key_exists('jobId', $opts) && $opts['jobId'] !== null;
        $id = $explicit ? $opts['jobId'] : ($this->idGenerator)();
        if (!\is_string($id) || $id === '' || \strlen($id) > 1_024 || str_contains($id, ':')) {
            throw new \InvalidArgumentException(
                'flow jobId must be non-empty, at most 1024 bytes and cannot contain a colon'
            );
        }
        if (isset($seen[$id])) {
            throw new \InvalidArgumentException("duplicate flow job id: {$id}");
        }
        $seen[$id] = true;
        return [$id, $explicit];
    }

    /**
     * @param array<string, mixed> $node
     * @param array<string, mixed> $internal
     * @return array<string, mixed>
     */
    private function dataFor(array $node, array $internal): array
    {
        $raw = $node['data'] ?? null;
        $data = \is_array($raw) && ($raw === [] || !array_is_list($raw))
            ? $raw
            : ($raw === null ? [] : ['payload' => $raw]);
        foreach ($data as $key => $_value) {
            if (!\is_string($key)) {
                throw new \InvalidArgumentException('flow job data requires string keys');
            }
            if ($key === 'name' || str_starts_with($key, '__')) {
                throw new \InvalidArgumentException("flow job data key is reserved: {$key}");
            }
        }
        $data['name'] = $node['name'];
        foreach ($internal as $key => $value) {
            $data[$key] = $value;
        }
        return $data;
    }

    /**
     * @param array<string, mixed> $opts
     * @param array<string, mixed> $data
     * @param list<string> $dependsOn
     * @param list<string> $childrenIds
     * @return array<string, mixed>
     */
    private function input(
        array $opts,
        array $data,
        string $id,
        ?string $parentId,
        array $dependsOn,
        array $childrenIds,
    ): array {
        foreach (self::TOPOLOGY as $key) {
            if (\array_key_exists($key, $opts)) {
                throw new \InvalidArgumentException(
                    'flow topology options are owned by FlowProducer'
                );
            }
        }
        foreach (self::UNSUPPORTED as $key) {
            if (\array_key_exists($key, $opts) && $opts[$key] !== null) {
                $kind = str_starts_with($key, 'debounce')
                    ? 'debounce'
                    : ($key === 'repeat' ? 'repeat' : 'deduplication');
                throw new \InvalidArgumentException(
                    "{$kind} is not supported inside an atomic flow"
                );
            }
        }
        $wire = Options::toWire($opts);
        $explicit = \array_key_exists('jobId', $opts) && $opts['jobId'] !== null;
        unset($wire['jobId']);
        $input = ['data' => $data, ...$wire];
        if ($explicit) {
            $input['customId'] = $id;
        }
        if ($parentId !== null) {
            $input['parentId'] = $parentId;
        }
        if ($dependsOn !== []) {
            $input['dependsOn'] = $dependsOn;
        }
        if ($childrenIds !== []) {
            $input['childrenIds'] = $childrenIds;
        }
        return Protocol::compact($input);
    }

    private static function randomId(): string
    {
        return bin2hex(random_bytes(16));
    }
}
