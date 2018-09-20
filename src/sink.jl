export SinkConfig, SINKCONFIG, collect_by_names

struct SinkConfig end

const SINKCONFIG = SinkConfig()


# # Reference implementation for sinks: `Vector`

makesink(::SinkConfig, elt) = [elt]

store!_or_reallocate(::SinkConfig, sink::Vector{T}, elt::S) where {T, S <: T} = push!(sink, elt)

store!_or_reallocate(::SinkConfig, sink::Vector, elt) = vcat(sink, [elt])

finalize(::SinkConfig, sink::Vector) = sink


# # Collecting named tuples

function collect_by_names(cfg::SinkConfig, itr)
    y = iterate(itr)
    y ≡ nothing && return nothing
    elts, newstate = y
    @argcheck elts isa NamedTuple
    sinks = map(elt -> makesink(cfg::SinkConfig, elt), elts)
    collect_by_names!(sinks, cfg, itr, newstate)
end

function collect_by_names!(sinks::NamedTuple{K}, cfg, itr, state) where K
    while true
        y = iterate(itr, state)
        y ≡ nothing && return map(sink -> finalize(cfg, sink), sinks)
        elts, state = y
        newsinks = map((sink, elt) -> store!_or_reallocate(cfg, sink, elt), sinks, elts)
        sinks ≡ newsinks || return collect_by_names!(newsinks, cfg, itr, state)
    end
end
