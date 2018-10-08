export SinkConfig, SINKCONFIG, SINKVECTORS, collect_sink, collect_sinks, RLEVector,
    contiguous_blocks

struct SinkConfig{M}
    useRLE::Bool
    missingvalue::M
end

SinkConfig(; useRLE = true, missingvalue = missing) = SinkConfig(useRLE, missingvalue)

"Default sink configuration."
const SINKCONFIG = SinkConfig(;)

"Sink configuration that collects to vectors."
const SINKVECTORS = SinkConfig(; useRLE = false)

"""
$(SIGNATURES)

Make sinks for a (named) tuple pf elements.
"""
make_sinks(cfg, elts::Union{Tuple, NamedTuple}) = map(elt -> make_sink(cfg, elt), elts)

"""
$(SIGNATURES)

Finalize a (named) tuple of sinks.
"""
finalize_sinks(cfg, sinks::Union{Tuple, NamedTuple}) =
    map(sink -> finalize_sink(cfg, sink), sinks)


# # Reference implementation for sinks: `Vector`

function make_sink(cfg::SinkConfig, elt)
    if cfg.useRLE
        RLEVector{typeof(cfg.missingvalue)}(Int8, elt)
    else
        [narrow(elt)]
    end
end

function store!_or_reallocate(::SinkConfig, sink::Vector{T}, elt) where T
    if cancontain(T, elt)
        (push!(sink, elt); sink)
    else
        append1(sink, narrow(elt))
    end
end

finalize_sink(::SinkConfig, sink::Vector) = sink


# # RLE compressed vector

struct RLEVector{C,T,S}
    counts::Vector{C}
    data::Vector{T}
    function RLEVector{S}(counts::Vector{C}, data::Vector{T}) where {C <: Signed, T, S}
        @argcheck isconcretetype(S) && fieldcount(S) == 0 "$(S) is not a concrete singleton type."
        @argcheck length(counts) ≥ length(data)
        new{eltype(counts), eltype(data) ,S}(counts, data)
    end
end

RLEVector{S}(C::Type{<:Signed}, elt) where S = RLEVector{S}(ones(C, 1), [narrow(elt)])

function store!_or_reallocate(::SinkConfig, sink::RLEVector{C,T,S}, elt) where {C,T,S}
    @unpack counts, data = sink
    if cancontain(T, elt)       # can accommodate elt, same sink
        if data[end] == elt && 0 < counts[end] < typemax(C)
            counts[end] += one(C) # increment existing count
        else
            push!(counts, one(C)) # start new RLE run
            push!(data, elt)
        end
        sink
    else                        # can't accommodate elt, allocate new sink
        RLEVector{S}(append1(counts, one(C)), append1(data, narrow(elt)))
    end
end

function store!_or_reallocate(::SinkConfig, sink::RLEVector{C,T,S}, elt::S) where {C,T,S}
    @unpack counts, data = sink
    if 0 > counts[end] > typemin(C) # ongoing RLE run with S
        counts[end] -= one(C)
    else
        push!(counts, -one(C))  # start new RLE runx
    end
    sink
end

finalize_sink(::SinkConfig, rle::RLEVector) = rle

eltype(::RLEVector{C,T,S}) where {C,T,S} = Base.promote_typejoin(T,S)

length(rle::RLEVector{C,T,S}) where {C,T,S} = sum(abs ∘ Int, rle.counts)

function iterate(rle::RLEVector{C,T,S},
                 (countsindex, dataindex, remaining) = (0, 0, zero(C))) where {C,T,S}
    @unpack counts, data = rle
    if remaining < 0
        (S(), (countsindex, dataindex, remaining + one(C)))
    elseif remaining > 0
        (data[dataindex], (countsindex, dataindex, remaining - one(C)))
    else
        countsindex += 1
        countsindex > length(counts) && return nothing
        remaining = counts[countsindex]
        if remaining > 0
            dataindex += 1
        end
        iterate(rle, (countsindex, dataindex, remaining))
    end
end


# # Collecting named tuples

function collect_sink(cfg::SinkConfig, itr)
    y = iterate(itr)
    y ≡ nothing && return nothing
    elt, state = y
    collect_sink!(make_sink(cfg::SinkConfig, elt), cfg, itr, state)
end

function collect_sink!(sink, cfg::SinkConfig, itr, state)
    while true
        y = iterate(itr, state)
        y ≡ nothing && return finalize_sink(cfg, sink)
        elt, state = y
        newsink = store!_or_reallocate(cfg, sink, elt)
        sink ≡ newsink || return collect_sink!(newsink, cfg, itr, state)
    end
end

function collect_sinks(cfg::SinkConfig, itr)
    y = iterate(itr)
    y ≡ nothing && return nothing
    elts, newstate = y
    @argcheck elts isa NamedTuple
    sinks = make_sinks(cfg, elts)
    collect_sinks!(sinks, cfg, itr, newstate)
end

function collect_sinks!(sinks::NamedTuple, cfg, itr, state)
    while true
        y = iterate(itr, state)
        y ≡ nothing && return finalize_sinks(cfg, sinks)
        elts, state = y
        newsinks = map((sink, elt) -> store!_or_reallocate(cfg, sink, elt), sinks, elts)
        sinks ≡ newsinks || return collect_sinks!(newsinks, cfg, itr, state)
    end
end

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

IteratorSize(::ContiguousBlockIterator) = Base.SizeUnknown()

IteratorEltype(::ContiguousBlockIterator) = Base.EltypeUnknown()

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
    _collect_blocks!(sinks, b, firstkey, state)
end

function iterate(b::ContiguousBlockIterator, state)
    state ≡ nothing && return nothing
    sinks, firstkey, itrstate = state
    _collect_blocks!(sinks, b, firstkey, itrstate)
end

function _collect_blocks!(sinks::NamedTuple, b::ContiguousBlockIterator, firstkey, state)
    @unpack cfg, f, itr = b
    while true
        y = iterate(itr, state)
        y ≡ nothing && return firstkey => finalize_sinks(cfg, sinks), nothing
        x, state = y
        key, elts = f(x)
        key == firstkey || return firstkey => finalize_sinks(cfg, sinks), (make_sinks(cfg, elts), key, state)
        newsinks = map((sink, elt) -> store!_or_reallocate(cfg, sink, elt), sinks, elts)
        sinks ≡ newsinks || return _collect_blocks!(newsinks, b, firstkey, state)
    end
end
