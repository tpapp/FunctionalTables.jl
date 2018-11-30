export by, GroupedColumn, map_nongrouped

####
#### GroupedColumn type
####

"""
GroupedColumn(value, len)

A representation of a value that was used for grouping. Equivalent to a vector containing
`len` instances of `value` with additional semantics that is by convention in this package.
See [`map_nongrouped`].
"""
struct GroupedColumn{T} <: AbstractVector{T}
    value::T
    len::Int
end

Base.size(s::GroupedColumn) = (s.len, )

Base.IndexStyle(::Type{<:GroupedColumn}) = Base.IndexLinear()

function Base.getindex(s::GroupedColumn, i::Integer)
    @boundscheck checkbounds(s, i)
    s.value
end

groupedkeys(ft::FunctionalTable) =
    tuple((k for (k, c) in pairs(ft.columns) if c isa GroupedColumn)...)

"""
$(SIGNATURES)

Split the grouped and nongrouped columns of `f`, apply `f` to the nongrouped columns, then
merge with grouped columns first. For `replace`, see [`merge`](@ref).
"""
function map_nongrouped(f, ft::FunctionalTable; replace = false)
    K = groupedkeys(ft)
    merge(select(ft, K), f(select(ft; drop = K)); replace = replace)
end

map_nongrouped(f; replace = false) = ft -> map_nongrouped(f, ft; replace = replace)

####
#### iterating grouped values (implementation, internal)
####

"""
$(TYPEDEF)

Implements [`by`](@ref).

Iterator state is a tuple, with

1. `sinks` and `firstkey`, created from the element with a non-matching key,

2. `itrstate`, the iteration state for `itr`.
"""
struct GroupedTable{K, T <: FunctionalTable, C <: SinkConfig}
    ft::T
    cfg::C
    function GroupedTable{K}(ft::T, cfg::C) where {K, T <: FunctionalTable, C <: SinkConfig}
        checkvalidkeys(K, keys(getsorting(ft))) # FIXME rethink: is all that is needed?
        new{K, T, C}(ft)
    end
end

IteratorSize(::Type{<:GroupedTable}) = Base.SizeUnknown()

# FIXME type may be known to a certain extent, <: FunctionalTable?
IteratorEltype(::Type{<:GroupedTable}) = Base.EltypeUnknown()

getsorting(g::GroupedTable{K}) where K = select_sorting(getsorting(g.ft), K)

function iterate(g::GroupedTable{K}) where K
    @unpack ft, cfg = g
    row, itrstate = @ifsomething iterate(ft)
    firstkey, elts = split_namedtuple(NamedTuple{K}, row)
    sinks = make_sinks(cfg, elts)
    _collect_block!(sinks, g, firstkey, itrstate)
end

function iterate(g::GroupedTable, state)
    sinks, firstkey, itrstate = @ifsomething state
    _collect_block!(sinks, g, firstkey, itrstate)
end

function _collect_block!(sinks::NamedTuple, g::GroupedTable{K}, firstkey, state) where {K}
    @unpack ft, cfg = g
    function _grouped()
        # NOTE: Helper function that finalizes the sinks and makes a table of the grouped
        # and nongrouped values.
        ft_nongrouped = FunctionalTable(finalize_sinks(cfg, sinks))
        len = length(ft_nongrouped)
        ft_grouped = FunctionalTable(map(v -> GroupedColumn(v, len), firstkey))
        # FIXME: some residual sorting remains, use? should be calculated in constructor.
        merge(ft_grouped, ft_nongrouped)
    end
    while true
        y = iterate(ft, state)
        y ≡ nothing && return _grouped(), nothing
        row, state = y
        key, elts = split_namedtuple(NamedTuple{K}, row)
        key == firstkey || return _grouped(), (make_sinks(cfg, elts), key, state)
        newsinks = map((sink, elt) -> store!_or_reallocate(cfg, sink, elt), sinks, elts)
        sinks ≡ newsinks || return _collect_block!(newsinks, g, firstkey, state)
    end
end

####
#### by and its implementation
####

by(ft::FunctionalTable, groupkeys::Keys; cfg = SINKVECTORS) =
    GroupedTable{groupkeys}(ft, cfg)

"""
$(SIGNATURES)

Group rows by columns `groupkeys`, apply `f`, then combine into a FunctionalTable.

`f` receives a `FunctionalTable`, collected with `cfg`. It is supposed to return an
*iterable* that returns rows. These will be collected into a `FunctionalTable` with
`outer_cfg`.

When `f` returns just a single row (eg aggregation), wrap by `Ref` to create a
single-element iterable. See also `map_ungrouped`.
"""
function by(f, ft::FunctionalTable, groupkeys::Keys;
            cfg = SINKVECTORS, outer_cfg = SINKCONFIG)
    # FIXME: 1. custom sorting override?
    FunctionalTable(Iterators.flatten(imap(f, by(ft, groupkeys; cfg = cfg))),
                    getsorting(ft), SORTING_TRY; cfg = outer_cfg)
end
