<?php

declare(strict_types=1);

namespace Bunqueue;

/** Flow result node: the created job plus its (optional) children nodes. */
final class FlowNode
{
    /** @param list<FlowNode> $children */
    public function __construct(
        public readonly Job $job,
        public readonly array $children = [],
    ) {
    }
}
