export SinkConfig, SINKVECTORS


# sinks — general interface

# A *sink* is a container that collects elements, expanding as necessary. When the
# collection is finished, sinks are *finalized* into *columns*. Details are governed by sink
# configuration objects (see [`SinkConfig`](@ref).
#
# Interface for sinks
#
# 1. [`make_sink`](@ref) for creating a sink for a single element.
#
# 2. [`store_or_reallocate!`](@ref) for saving another element. Either the result is `≡` to
# the sink in the argument, or a new sink was reallocated (potentially changing type).
#
# 3. [`finalize_sink`](@ref) turns the sink into a *column*, which is an iterable with a
# length and element type, supports `iterate,` but is not necessarily optimized for random
# access.

"""
$(TYPEDEF)
"""
struct SinkConfig{M}
    useRLE::Bool
    missingvalue::M
end

"""
$(SIGNATURES)

Make a sink configuration, using defaults.
"""
SinkConfig(; useRLE = true, missingvalue = missing) = SinkConfig(useRLE, missingvalue)

"Default sink configuration."
const SINKCONFIG = SinkConfig(;)

"Sink configuration that collects to vectors."
const SINKVECTORS = SinkConfig(; useRLE = false)


# helper functions

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

"""
$(SIGNATURES)

Create and return a sink using configuration `cfg` that stores `elt`.
"""
function make_sink(cfg::SinkConfig, elt)
    if cfg.useRLE
        RLEVector{typeof(cfg.missingvalue)}(Int8, elt)
    else
        [narrow(elt)]
    end
end

"""
$(SIGNATURES)

Either store `elt` in `sink` (in which case the returned value is `≡ sink`), or
allocate a new sink that can do this, copy the contents, save `elt` and return that (then
the returned value is `≢ sink`).
"""
function store!_or_reallocate(::SinkConfig, sink::Vector{T}, elt) where T
    if cancontain(T, elt)
        (push!(sink, elt); sink)
    else
        append1(sink, narrow(elt))
    end
end

"""
$(SIGNATURES)

Convert `sink` to a *column*.

`sink` may share structure with the result and is not supposed to be used for saving any
more elements.
"""
finalize_sink(::SinkConfig, sink::Vector) = sink


# # RLE compressed vector

"""
$(TYPEDEF)

An RLE encoded vector, using negative lengths for missing values.

When an elemenet in `counts` is positive, it encodes that many of the corresponding element
in `data`.

Negative `counts` encode missing values of type `S` (has to be a concrete singleton). In
this case there is no corresponding value in `data`, ie `data` may have *fewer elements*
than `counts`.

An RLEVector can also act as a column.
"""
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

Base.eltype(::RLEVector{C,T,S}) where {C,T,S} = Base.promote_typejoin(T,S)

Base.length(rle::RLEVector{C,T,S}) where {C,T,S} = sum(abs ∘ Int, rle.counts)

function Base.iterate(rle::RLEVector{C,T,S},
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

####
#### Collecting named tuples
####

"""
$(TYPEDEF)

Wrapper type to indicate that the length should not be checked.

!!! note
    The perfect footgun. Only use when the lengths are known and verified by construction.
"""
struct TrustLength
    len::Int
end

"""
$(SIGNATURES)

Collect results from `itr` into a sink (using config `cfg`), then finalize and return the
column.
"""
function collect_column(cfg::SinkConfig, itr)
    y = iterate(itr)
    y ≡ nothing && return nothing
    elt, state = y
    collect_column!(make_sink(cfg::SinkConfig, elt), cfg, itr, state)
end

function collect_column!(sink, cfg::SinkConfig, itr, state)
    while true
        y = iterate(itr, state)
        y ≡ nothing && return finalize_sink(cfg, sink)
        elt, state = y
        newsink = store!_or_reallocate(cfg, sink, elt)
        sink ≡ newsink || return collect_column!(newsink, cfg, itr, state)
    end
end

"""
len, columns, ordering_rule = $(SIGNATURES)

Collect results from `itr`, which are supposed to be `NamedTuple`s with the same names, into
sinks (using config `cfg`), then finalize and return

1. the length,

2. the `NamedTuple` of the columns, and

3. the ordering rule (which is always `::TrustOrdering`, by construction).

The results can be
"""
function collect_columns(cfg::SinkConfig, itr, ordering_rule::OrderingRule{R}) where R
    elts, state = @ifsomething iterate(itr)
    @argcheck elts isa NamedTuple
    sinks = make_sinks(cfg, elts)
    if R ≡ :try
        # we need to narrow ordering so that comparisons make sense
        ordering_rule = OrderingRule{R}(select_ordering(ordering_rule.ordering, keys(elts)))
    end
    collect_columns!(sinks, 1, cfg, itr, ordering_rule,
                     # :trust, we don't need the last element for comparison, hence the ()
                     R ≡ :trust ? () : elts, state)
end

function collect_columns!(sinks::NamedTuple, len::Int, cfg, itr,
                          ordering_rule::OrderingRule{R}, lastelts, state) where R
    @unpack ordering = ordering_rule
    while true
        y = iterate(itr, state)
        y ≡ nothing && return TrustLength(len), finalize_sinks(cfg, sinks), TrustOrdering(ordering_rule)
        elts, state = y
        newsinks = map((sink, elt) -> store!_or_reallocate(cfg, sink, elt), sinks, elts)
        len += 1
        if R ≢ :trust
            if cmp_ordering(ordering, lastelts, elts) > 0
                if R ≡ :verify
                    error("Sorting $(sorting) violated: $(lastelts) ≰ $(elts).")
                else # R ≡ :try
                    new_ordering = retained_ordering(ordering, lastelts, elts)
                    return collect_columns!(newsinks, len, cfg, itr,
                                            OrderingRule{R}(new_ordering), elts, state)
                end
            end
            lastelts = elts
        end
        sinks ≡ newsinks || return collect_columns!(newsinks, len, cfg, itr, ordering_rule,
                                                    lastelts, state)
    end
end
