export SinkConfig, SINKCONFIG, collect_sink, collect_sinks, RLEVector

struct SinkConfig{M}
    useRLE::Bool
    missingvalue::M
end

SinkConfig(; useRLE = true, missingvalue = missing) = SinkConfig(useRLE, missingvalue)

const SINKCONFIG = SinkConfig(;)


# # Reference implementation for sinks: `Vector`

function makesink(cfg::SinkConfig, elt)
    if cfg.useRLE
        RLEVector{typeof(cfg.missingvalue)}(Int8, elt)
    else
        [elt]
    end
end

function store!_or_reallocate(::SinkConfig, sink::Vector{T}, elt::S) where {T, S}
    if cancontain(T, S)
        (push!(sink, elt); sink)
    else
        append1(sink, elt)
    end
end

finalize(::SinkConfig, sink::Vector) = sink


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

RLEVector{S}(C::Type{<:Signed}, elt) where S = RLEVector{S}(ones(C, 1), [elt])

function store!_or_reallocate(::SinkConfig, sink::RLEVector{C,T,S}, elt::E) where {C,T,S,E}
    @unpack counts, data = sink
    if cancontain(T, E)         # can accommodate elt, same sink
        if data[end] == elt && 0 < counts[end] < typemax(C)
            counts[end] += one(C) # increment existing count
        else
            push!(counts, one(C)) # start new RLE run
            push!(data, elt)
        end
        sink
    else                        # can't accommodate elt, allocate new sink
        RLEVector{S}(append1(counts, one(C)), append1(data, elt))
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

finalize(::SinkConfig, rle::RLEVector) = rle

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
    collect_sink!(makesink(cfg::SinkConfig, elt), cfg, itr, state)
end

function collect_sink!(sink, cfg::SinkConfig, itr, state)
    while true
        y = iterate(itr, state)
        y ≡ nothing && return finalize(cfg, sink)
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
    sinks = map(elt -> makesink(cfg::SinkConfig, elt), elts)
    collect_sinks!(sinks, cfg, itr, newstate)
end

function collect_sinks!(sinks::NamedTuple, cfg, itr, state)
    while true
        y = iterate(itr, state)
        y ≡ nothing && return map(sink -> finalize(cfg, sink), sinks)
        elts, state = y
        newsinks = map((sink, elt) -> store!_or_reallocate(cfg, sink, elt), sinks, elts)
        sinks ≡ newsinks || return collect_sinks!(newsinks, cfg, itr, state)
    end
end
