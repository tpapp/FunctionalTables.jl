export groupbykeys

"""
$(TYPEDEF)

Implements [`contiguous_blocks`](@ref).

Iterator state is a tuple, with

1. `sinks` and `firstkey`, created from the element with a non-matching key,

2. `itrstate`, the iteration state for `itr`.
"""
struct ContiguousBlockIterator{C, F, T}
    cfg::C
    f::F
    itr::T
end

IteratorSize(::Type{<:ContiguousBlockIterator}) = Base.SizeUnknown()

IteratorEltype(::Type{<:ContiguousBlockIterator}) = Base.EltypeUnknown()

"""
$(SIGNATURES)

Return an iterator that maps elements `x` returned by another iterator `itr` with

```julia
key, elts = f(x)
```

and returns elements `key => block`, where `block` is a contiguous block of `elts`s for
which `key` is the same (when compared with `==`). `v`s are expected to be named tuples, and
collected into sinks which are then finalized.  `cfg` governs this.
"""
contiguous_blocks(f, itr; cfg = SINKVECTORS) = ContiguousBlockIterator(cfg, f, itr)

function iterate(b::ContiguousBlockIterator)
    @unpack cfg, f, itr = b
    y = iterate(itr)
    y ≡ nothing && return nothing
    x, state = y
    firstkey, elts = f(x)
    sinks = make_sinks(cfg, elts)
    _collect_block!(sinks, b, firstkey, state)
end

function iterate(b::ContiguousBlockIterator, state)
    state ≡ nothing && return nothing
    sinks, firstkey, itrstate = state
    _collect_block!(sinks, b, firstkey, itrstate)
end

function _collect_block!(sinks::NamedTuple, b::ContiguousBlockIterator, firstkey, state)
    @unpack cfg, f, itr = b
    while true
        y = iterate(itr, state)
        y ≡ nothing && return firstkey => finalize_sinks(cfg, sinks), nothing
        x, state = y
        key, elts = f(x)
        key == firstkey || return firstkey =>
            finalize_sinks(cfg, sinks), (make_sinks(cfg, elts), key, state)
        newsinks = map((sink, elt) -> store!_or_reallocate(cfg, sink, elt), sinks, elts)
        sinks ≡ newsinks || return _collect_block!(newsinks, b, firstkey, state)
    end
end

"""
$(SIGNATURES)

Return an iterator that returns `keys => functionaltable` pairs of rows which have the same
fields selected by `groupkeys` (which then form `keys`).
"""
function groupbykeys(groupkeys::Keys, itr; sorting = (), cfg = SINKVECTORS)
    splitter = NamedTupleSplitter(groupkeys)
    imap(((k, cols),) -> k => FunctionalTable(cols; sorting = sorting),
         contiguous_blocks(splitter, itr; cfg = cfg))
end
