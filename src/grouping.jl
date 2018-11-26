export groupby, GroupedTable

"""
$(TYPEDEF)
"""
struct GroupedTable{K <: NamedTuple, T <: FunctionalTable}
    grouping::K
    ft::T
end

function FunctionalTable(g::GroupedTable)
    @unpack grouping, ft = g
    len = length(ft)
    groupcols = NamedTuple{keys(grouping)}(Iterators.repeated(c, len) for c in values(grouping))
    FunctionalTable(merge(groupcols, ft.columns))
end

## FIXME: is direct iteration needed?
# function iterate(g::GroupedTable, state...)
#     @unpack grouping, ft = groupedblock
#     row, state = @ifsomething iterate(ft, state...)
#     merge(grouping, row), state
# end

"""
$(TYPEDEF)

Implements [`groupby`](@ref).

Iterator state is a tuple, with

1. `sinks` and `firstkey`, created from the element with a non-matching key,

2. `itrstate`, the iteration state for `itr`.
"""
struct GroupedBlocks{K, T, C}
    ft::T
    cfg::C
end

IteratorSize(::Type{<:GroupedBlocks}) = Base.SizeUnknown()

# FIXME type is known to a certain extent, as a subtype of GroupedBlock
IteratorEltype(::Type{<:GroupedBlocks}) = Base.EltypeUnknown()

getsorting(g::GroupedBlocks{K}) where K = select_sorting(getsorting(g.ft), K)

"""
$(SIGNATURES)
"""
function groupby(groupkeys::Keys, ft::FunctionalTable; cfg = SINKVECTORS)
    checkvalidkeys(groupkeys, ft)
    GroupedBlocks{groupkeys, typeof(ft), typeof(cfg)}(ft, cfg)
end

function iterate(g::GroupedBlocks{K}) where K
    @unpack ft, cfg = g
    row, itrstate = @ifsomething iterate(ft)
    firstkey, elts = split_namedtuple(NamedTuple{K}, row)
    sinks = make_sinks(cfg, elts)
    _collect_block!(sinks, g, firstkey, itrstate)
end

function iterate(g::GroupedBlocks, state)
    sinks, firstkey, itrstate = @ifsomething state
    _collect_block!(sinks, g, firstkey, itrstate)
end

function _collect_block!(sinks::NamedTuple, g::GroupedBlocks{K}, firstkey, state) where {K}
    @unpack ft, cfg = g
    _grouped() = GroupedTable(firstkey, FunctionalTable(finalize_sinks(cfg, sinks)))
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
