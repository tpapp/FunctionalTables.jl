#####
##### Interface and implementation for split-apply-combine.
#####

export by, RepeatRow

####
#### RepeatValue type
####

"""
RepeatValue(value, len)

Equivalent to a vector containing `len` instances of `value`. Used *internally*.
"""
struct RepeatValue{T} <: AbstractVector{T}
    value::T
    len::Int
end

Base.size(s::RepeatValue) = (s.len, )

Base.IndexStyle(::Type{<:RepeatValue}) = Base.IndexLinear()

function Base.getindex(s::RepeatValue, i::Integer)
    @boundscheck checkbounds(s, i)
    s.value
end

"""
RepeatRow(row)

A row repeated as many times as needed. Can be `merge`d to a `FunctionalTable`, or
instantiated with `FunctionalTable(len, repeat_row)`.
"""
struct RepeatRow{T <: NamedTuple}
    row::T
end

function FunctionalTable(len::Int, repeat_row::RepeatRow,
                         ordering::OrderingRule = TrustOrdering())
    FunctionalTable(TrustLength(len), map(v -> RepeatValue(v, len), repeat_row.row),
                    ordering)
end

Base.merge(row::RepeatRow, ft::FunctionalTable; kwargs...) =
    merge(FunctionalTable(ft.len, row), ft; kwargs...)

Base.merge(ft::FunctionalTable, row::RepeatRow; kwargs...) =
    merge(ft, FunctionalTable(ft.len, row); kwargs...)

"""
$(SIGNATURES)

Prepend the `index` as repeated columns to `f(index, tables...)`.
"""
fuse(f, index::NamedTuple, tables...) =
    merge(RepeatRow(index), FunctionalTable(f(index, tables...)))

"""
$(TYPEDEF)

Implements [`by`](@ref).

Iterator state is a tuple, with

1. `sinks` and `firstkey`, created from the element with a non-matching key,

2. `itrstate`, the iteration state for `itr`.
"""
struct SplitTable{K, T <: FunctionalTable, C <: SinkConfig}
    ft::T
    cfg::C
    function SplitTable{K}(ft::T, cfg::C) where {K, T <: FunctionalTable, C <: SinkConfig}
        @argcheck is_prefix(K, orderkey.(ft.ordering))
        new{K, T, C}(ft)
    end
end

Base.IteratorSize(::Type{<:SplitTable}) = Base.SizeUnknown()

# FIXME type may be known to a certain extent, <: FunctionalTable?
Base.IteratorEltype(::Type{<:SplitTable}) = Base.EltypeUnknown()

ordering(st::SplitTable{K}) where K = select_ordering(ordering(st.ft), K)

function Base.iterate(g::SplitTable{K}) where K
    @unpack ft, cfg = g
    row, itrstate = @ifsomething iterate(ft)
    firstkey, elts = split_namedtuple(NamedTuple{K}, row)
    sinks = make_sinks(cfg, elts)
    _collect_block!(sinks, 1, g, firstkey, itrstate)
end

function Base.iterate(g::SplitTable, state)
    sinks, firstkey, itrstate = @ifsomething state
    _collect_block!(sinks, 1, g, firstkey, itrstate)
end

function _collect_block!(sinks::NamedTuple, len::Int, g::SplitTable{K}, firstkey, state) where {K}
    @unpack ft, cfg = g
    _grouped() = (firstkey, FunctionalTable(TrustLength(len), finalize_sinks(cfg, sinks),
                                            # FIXME residual ordering from split table
                                            # should be propagated
                                            TrustOrdering()))
    while true
        y = iterate(ft, state)
        y ≡ nothing && return _grouped(), nothing
        row, state = y
        key, elts = split_namedtuple(NamedTuple{K}, row)
        key == firstkey || return _grouped(), (make_sinks(cfg, elts), key, state)
        newsinks = map((sink, elt) -> store!_or_reallocate(cfg, sink, elt), sinks, elts)
        len += 1
        sinks ≡ newsinks || return _collect_block!(newsinks, len, g, firstkey, state)
    end
end

####
#### by and its implementation
####

"""
$(SIGNATURES)

An iterator that groups rows of tables by the columns `splitkeys`, returning
`(index::NamedTupe, table::FunctionalTable)` for each contiguous block of the index keys.

`cfg` is used for collecting `table`.
"""
by(ft::FunctionalTable, splitkeys::Keys; cfg = SINKVECTORS) = SplitTable{splitkeys}(ft, cfg)

by(ft::FunctionalTable, splitkeys::Symbol...; kwargs...) = by(ft, splitkeys; kwargs...)

"""
$(SIGNATURES)

Map a table split with [`by`](@ref) using `f`.

Specifically, `f(index, table)` receives the split index (a `NamedTuple`) and a
`FunctionalTable`.

It is supposed to return an *iterable* that returns rows (can be a `FunctionalTable`). These
will be prepended with the corresponding index, and collected into a `FunctionalTable` with
`cfg`.

When `f` returns just a single row (eg aggregation), wrap by `Ref` to create a
single-element iterable.
"""
function Base.map(f, st::SplitTable; cfg = SINKCONFIG)
    # FIXME: idea: custom ordering override? would that make sense?
    FunctionalTable(Iterators.flatten(imap(args -> fuse(f, args...), st)),
                    TrustOrdering(ordering(st)); cfg = cfg)
end
